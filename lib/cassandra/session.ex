defmodule Cassandra.Session do
  @moduledoc """
  Session is a server for handling query execution.
  """

  use GenServer

  require Logger

  alias Cassandra.{Cluster, Host, LoadBalancing, Reconnection}
  alias Cassandra.Session.Worker

  @defaults [
    reconnection_policy: Reconnection.Exponential,
    reconnection_args: [],
    async_init: true,
  ]

  @default_balancer_policy LoadBalancing.RoundRobin

  ### Client API ###

  @doc """
  Starts a session on the `cluster` with given `options`, linked to current process.

  ## Options

    * `:balancer_policy` - Cassandra.LoadBalancing.Policy to use
    * `:balancer_args` - list of arguments to pass to `:balancer_policy` struct
    * `:port` - Cassandra native protocol port (default: `9042`)
    * `:connection_timeout` - connection timeout in milliseconds (defult: `5000`)
    * `:timeout` - query execution timeout in milliseconds (default: `:infinity`)
    * `:keyspace` - name of keyspace to bind session to
    * `:reconnection_policy` - module which implements Cassandra.Reconnection.Policy (defult: `Exponential`)
    * `:reconnection_args` - list of arguments to pass to `:reconnection_policy` on init (defult: `[]`)

  Retutns `{:ok, pid}` or `{:error, reason}`.
  """
  def start_link(cluster, options \\ [], gen_server_options \\ []) do
    GenServer.start_link(__MODULE__, [cluster, options], gen_server_options)
  end

  @doc false
  def notify(session, message) do
    GenServer.cast(session, {:notify, message})
  end

  @doc """
  Executes the `query` with given `options` on `session`.

  If `query` is not prepared, `session` prepares it automatically.

  ## Return values

  `{:ok, :done}` when cassandra response is VOID in response to some queries

  `{:ok, data}` where data is one of the following structs:

    * `CQL.Result.SetKeyspace`
    * `CQL.Result.SchemaChange`
    * `CQL.Result.Rows`

  `{:error, {code, message}}` when cassandra response is an error
  """
  def execute(session, query, options \\ []) do
    GenServer.call(session, {:execute, query, options}, :infinity)
  end

  @doc """
  Prepares the `query` for later execution.

  It returns `{:ok, query}` on success and `{:error, {code, message}}` when cassandra response is an error
  """
  def prepare(session, statement) do
    GenServer.call(session, {:prepare, statement}, :infinity)
  end

  @doc """
  Sends `request` through `session`.

  `request` must be a `CQL.Request`.

  ## Return values

  `{:ok, :done}` when cassandra response is VOID in response to some queries

  `{:ok, :ready}` when cassandra response is READY in response to `Register` requests

  `{:ok, data}` where data is one of the following structs:

    * `CQL.Supported`
    * `CQL.Result.SetKeyspace`
    * `CQL.Result.SchemaChange`
    * `CQL.Result.Prepared`
    * `CQL.Result.Rows`

  `{:error, {code, message}}` when cassandra response is an error
  """
  def send(session, request) do
    GenServer.call(session, {:send, request}, :infinity)
  end

  ### GenServer Callbacks ###

  @doc false
  def init([cluster, options]) do
    task_supervisor = Keyword.get_lazy options, :task_supervisor, fn ->
      {:ok, pid} = Task.Supervisor.start_link
      pid
    end
    balancer_policy = Keyword.get(options, :balancer_policy, @default_balancer_policy)
    balancer_args = Keyword.get(options, :balancer_args, [])
    balancer = struct(balancer_policy, balancer_args)
    retry = Keyword.get(options, :retry, &retry?/3)
    retry_args = Keyword.get(options, :retry, [0])

    connection_options = Keyword.take(options, [
      :port,
      :connection_timeout,
      :timeout,
      :keyspace,
      :reconnection_policy,
      :reconnection_args,
    ])

    options =
      @defaults
      |> Keyword.merge(connection_options)
      |> Keyword.put(:session, self)

    Cluster.register(cluster, self)

    Kernel.send(self, :connect)

    state = %{
      cluster: cluster,
      options: options,
      balancer: balancer,
      retry: {retry, retry_args},
      task_supervisor: task_supervisor,
      hosts: %{},
      requests: [],
      statements: %{},
      refs: %{},
    }

    {:ok, state}
  end

  @doc false
  def handle_call({:send, request}, from, state) do
    handle_send(request, from, state)
  end

  @doc false
  def handle_call({:prepare, statement}, from, state) do
    prepare = %CQL.Prepare{query: statement}
    send_through_self(prepare, {from, statement}, state)
  end

  @doc false
  def handle_call({:execute, {statement, list}, options}, from, state)
  when is_bitstring(statement) and is_list(list) do
    prepare_and_execute(statement, list, options, from, state)
  end

  @doc false
  def handle_call({:execute, statement, options}, from, state)
  when is_bitstring(statement)
  do
    if has_values?(options) do
      prepare_and_execute(statement, nil, options, from, state)
    else
      query = %CQL.Query{query: statement, params: struct(CQL.QueryParams, options)}
      handle_send(query, from, state)
    end
  end

  @doc false
  def handle_cast({:notify, {change, {id, conn}}}, %{hosts: hosts} = state) do
    hosts = case change do
      :connection_opened ->
        update_in(hosts[id], &Host.toggle_connection(&1, conn, :open))

      :connection_closed ->
        update_in(hosts[id], &Host.toggle_connection(&1, conn, :close))

      :connection_stopped ->
        update_in(hosts[id], &Host.delete_connection(&1, conn))

      {:prepared, hash, prepared} ->
        update_in(hosts[id], &Host.put_prepared_statement(&1, hash, prepared))

      other ->
        Logger.warn("#{__MODULE__} unhandled notify #{inspect other}")
        hosts
    end

    state = %{state | hosts: hosts}

    state = if change == :connection_opened do
      send_requests(state)
    else
      state
    end

    {:noreply, state}
  end

  @doc false
  def handle_cast({:notify, {change, id}}, %{hosts: hosts, balancer: balancer} = state) do
    hosts = case change do
      :host_up ->
        host = hosts[id]
        existing = Enum.count(host.connections)
        expected = LoadBalancing.count(balancer, host)
        n = expected - existing
        if n >= 0 do
          updated_hosts =
            host
            |> start_connections(n, state.options)
            |> filter_map

          Map.merge(hosts, updated_hosts)
        else
          Logger.debug("#{__MODULE__} already connected to #{inspect id}")
          hosts
        end

      :host_down ->
        update_in(hosts[id], &Host.delete_prepared_statements(&1))

      other ->
        Logger.warn("#{__MODULE__} unhandled notify #{inspect other}")
        hosts
    end
    {:noreply, %{state | hosts: hosts}}
  end

  @doc false
  def handle_info(:connect, %{balancer: balancer, options: options} = state) do
    hosts =
      state.cluster
      |> Cluster.hosts
      |> Map.values
      |> Enum.reject(&Host.down?/1)
      |> Enum.flat_map(&start_connections(&1, LoadBalancing.count(balancer, &1), options))
      |> filter_map

    {:noreply, %{state | hosts: hosts}}
  end

  @doc false
  def handle_info({:DOWN, _ref, :process, conn, _reason}, %{hosts: hosts} = state) do
    Logger.warn("#{__MODULE__} connection lost")
    hosts =
      hosts
      |> Enum.map(fn {id, host} -> {id, Host.delete_connection(host, conn)} end)
      |> Enum.into(%{})
    {:noreply, %{state | hosts: hosts}}
  end

  @doc false
  def handle_info({ref, result}, state) do
    case pop_in(state.refs[ref]) do
      {nil, state} ->
        {:noreply, state}

      {{from, statement}, state} ->
        reply = case result do
          %{result: {:ok, _}}        -> {:ok, statement}
          %{result: {:error, error}} -> error
        end
        GenServer.reply(from, reply)
        {:noreply, state}

      {{from, prepare, batch_list, hash, options}, state} ->
        case result do
          %{result: {:ok, _}} ->
            execute(prepare, batch_list, hash, options, from, state)
          %{result: {:error, error}} ->
            GenServer.reply(from, error)
            {:noreply, state}
        end
    end
  end

  ### Helpers ###

  defp send_through_self(request, data, state) do
    ref = make_ref
    next_state = put_in(state.refs[ref], data)
    handle_send(request, {self, ref}, next_state)
  end

  defp handle_send(request, from, %{hosts: hosts} = state) do
    start_time = :erlang.monotonic_time
    if open_connections_count(hosts) < 1 do
      {:noreply, %{state | requests: [{from, request, start_time} | state.requests]}}
    else
      start_task({from, request, start_time}, hosts, state)
      {:noreply, state}
    end
  end

  defp prepare_and_execute(statement, batch_list, options, from, state) when is_bitstring(statement) do
    prepare = %CQL.Prepare{query: statement}
    encoded = CQL.encode(prepare)
    hash = :crypto.hash(:md5, encoded)
    execute(prepare, batch_list, hash, options, from, state)
  end

  defp execute(prepare, batch_list, hash, options, from, state) do
    start_time = :erlang.monotonic_time
    preferred_hosts =
      state.hosts
      |> Map.values
      |> Enum.filter(&Host.has_prepared?(&1, hash))

    if open_connections_count(preferred_hosts) == 0 do
      send_through_self(prepare, {from, prepare, batch_list, hash, options}, state)
    else
      host = hd(preferred_hosts)
      prepared = host.prepared_statements[hash]
      execute = case batch_list do
        nil ->
          %CQL.Execute{prepared: prepared, params: struct(CQL.QueryParams, options)}
        values when is_list(values) ->
          queries = Enum.map(values, &%CQL.BatchQuery{query: prepared, values: &1})
          struct(CQL.Batch, Keyword.put(options, :queries, queries))
      end
      start_task({from, execute, start_time}, preferred_hosts, state)
      {:noreply, state}
    end
  end

  defp has_values?(options) do
    count =
      options
      |> Keyword.get(:values, [])
      |> Enum.count

    count > 0
  end

  defp select(request, hosts, balancer) when is_map(hosts) do
    select(request, Map.values(hosts), balancer)
  end

  defp select(request, hosts, balancer) do
    hosts
    |> LoadBalancing.select(balancer, request)
    |> Enum.map(&key/1)
  end

  defp start_task({from, request, start_time}, hosts, state) do
    conns = select(request, hosts, state.balancer)
    Task.Supervisor.start_child state.task_supervisor, fn ->
      Worker.send_request(request, from, conns, state.retry, start_time)
    end
  end

  defp send_requests(%{hosts: hosts} = state) do
    state.requests
    |> Enum.reverse
    |> Enum.each(&start_task(&1, hosts, state))

    %{state | requests: []}
  end

  defp start_connections(host, n, options) when is_integer(n) and n > 0 do
    Enum.map(1..n, fn _ -> start_connection(host, options) end)
  end

  defp start_connections(_, _, _), do: []

  defp start_connection(host, options) do
    result =
      options
      |> Keyword.put(:host, host)
      |> Cassandra.Connection.start

    {result, host}
  end

  defp started?({{:ok, _}, _}), do: true
  defp started?(_), do: false

  defp to_host_conn_pair({{:ok, conn}, host}), do: {host, conn}

  defp key({k, _}), do: k

  defp value({_, v}), do: v

  defp open_connections_count(hosts) when is_map(hosts) do
    hosts
    |> Map.values
    |> open_connections_count
  end

  defp open_connections_count(hosts) do
    hosts
    |> Enum.map(&Host.open_connections_count(&1))
    |> Enum.reduce(0, &(&1 + &2))
  end

  defp filter_map(connections) do
    connections = Enum.filter_map(connections, &started?/1, &to_host_conn_pair/1)

    connections
    |> Enum.map(&value/1)
    |> Enum.each(&Process.monitor/1)

    connections
    |> Enum.group_by(&key/1, &value/1)
    |> Enum.map(fn {host, conns} -> {host.id, Host.put_connections(host, conns, :close)} end)
    |> Enum.into(%{})
  end

  defp retry?(_request, _result, 3), do: {false, nil}
  defp retry?(_request, result, n) do
    retry = case result do
      {:ok, _} -> false
      {:error, {:read_timeout, _}} -> true
      {:error, {_, _}} -> false # cql error
      {:error, _} -> true # connection error
    end
    if retry == true and n < 3 do
      {true, [n + 1]}
    else
      {false, []}
    end
  end
end
