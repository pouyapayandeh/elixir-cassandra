defmodule Cassandra.Session.ConnectionManager do
  use GenServer
  require Logger

  alias Cassandra.{LoadBalancing, Reconnection}
  alias Cassandra.Cluster.Schema

  @default_connection_options [
    reconnection: {Reconnection.Exponential, []},
    async_init: true,
  ]

  @valid_connection_options [
    :port,
    :connection_timeout,
    :timeout,
    :keyspace,
    :reconnection,
  ]

  ### API ###

  def start_link(schema, balancer, task_supervisor, name, options) do
    GenServer.start_link(__MODULE__, [schema, balancer, task_supervisor, options], name: name)
  end

  def connections(manager) do
    GenServer.call(manager, :connections)
  end

  def connections(manager, ips) do
    GenServer.call(manager, {:connections, ips})
  end

  def host(manager, connection) do
    GenServer.call(manager, {:host, connection})
  end

  def connect(manager, host) do
    GenServer.cast(manager, {:connect, host})
  end

  ### GenServer Callbacks ###

  @doc false
  def init([schema, balancer, task_supervisor, options]) do
    table_options = [
      :set,
      :protected,
      {:read_concurrency, true},
    ]

    connections_table = Keyword.get(options, :table_name, __MODULE__)
    connections_tid = :ets.new(connections_table, table_options)

    statements_table = Keyword.get(options, :statements_table, :"#{__MODULE__}.Statements")
    statements_tid = :ets.new(statements_table, table_options)

    options = Keyword.take(options, @valid_connection_options)

    self = self()
    connection_options =
      @default_connection_options
      |> Keyword.merge(options)
      |> Keyword.put(:manager, self)

    Task.Supervisor.start_child task_supervisor, fn ->
      IO.inspect(schema
      |> Schema.up_hosts)
      |> start_connections(balancer, connection_options)
    end

    state = %{
      schema: schema,
      balancer: balancer,
      task_supervisor: task_supervisor,
      connection_options: connection_options,
      connections_tid: connections_tid,
      statements_tid: statements_tid,
    }

    {:ok, state}
  end

  @doc false
  def handle_call(:connections, from, %{connections_tid: connections_tid} = state) do
    Task.Supervisor.start_child state.task_supervisor, fn ->
      connections = :ets.select(connections_tid, [{{:"$1", :_, :open}, [], [:"$1"]}])
      GenServer.reply(from, connections)
    end
    {:noreply, state}
  end

  @doc false
  def handle_call({:connections, ip_list}, from, %{connections_tid: connections_tid} = state) do
    Task.Supervisor.start_child state.task_supervisor, fn ->
      connections =
        ip_list
        |> List.wrap
        |> Enum.map(&:ets.select(connections_tid, [{{:"$1", :"$2", :open}, [{:"=:=", {:const, &1}, :"$2"}], [:"$1"]}]))
        |> Enum.concat

      GenServer.reply(from, connections)
    end
    {:noreply, state}
  end

  @doc false
  def handle_call({:host, connection}, from, %{connections_tid: connections_tid} = state) do
    Task.Supervisor.start_child state.task_supervisor, fn ->
      ip = :ets.lookup_element(connections_tid, connection, 2)
      GenServer.reply(from, ip)
    end
    {:noreply, state}
  end

  @doc false
  def handle_cast({:connect, ip}, %{schema: schema, balancer: balancer, connection_options: connection_options} = state) do
    schema
    |> Schema.host(ip)
    |> start_connections(balancer, connection_options)

    {:noreply, state}
  end

  @doc false
  def handle_info({:notify, change, ip, connection}, state) do
    case change do
      :connection_opened ->
        :ets.insert(state.connections_tid, {connection, ip, :open})

      :connection_closed ->
        :ets.update_element(state.connections_tid, connection, {3, :close})

      :connection_stopped ->
        :ets.delete(state.connections_tid, connection)

      {:prepared, hash, prepared} ->
        :ets.insert(state.statements_tid, {hash, prepared})

      other ->
        Logger.warn("#{__MODULE__} unhandled notify #{inspect other}")
    end

    {:noreply, state}
  end

  @doc false
  def handle_info({:event, {:host_found, {ip, _}}}, %{schema: schema, balancer: balancer, connection_options: connection_options} = state) do
    schema
    |> Schema.host(ip)
    |> start_connections(balancer, connection_options)

    {:noreply, state}
  end

  @doc false
  def handle_info({:event, _}, state) do
    {:noreply, state}
  end

  @doc false
  def handle_info({:DOWN, _ref, :process, connection, _reason}, state) do
    :ets.delete(state.connections_tid, connection)
    {:noreply, state}
  end

  ### Internals ###

  defp start_connections(hosts, balancer, connection_options) when is_list(hosts) do
    hosts
    |> List.wrap
    |> Enum.map(fn host -> {host.ip, LoadBalancing.count(balancer, host)} end)
    |> Enum.flat_map(&start_connection(&1, connection_options))
    |> Enum.each(fn
         {_host, {:ok, _connection}} ->
           :ok
         {host, {:error, reason}} ->
           Logger.warn("Connection to #{inspect host} failed with reason #{reason}")
       end)
  end

  defp start_connection({ip, count}, options) when count > 0 do
    Enum.map(1..count, fn _ -> start_connection(ip, options) end)
  end

  defp start_connection(ip, options) do
    result =
      options
      |> Keyword.put(:host, ip)
      |> Cassandra.Connection.start

    {ip, result}
  end
end
