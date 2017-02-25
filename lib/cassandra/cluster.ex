defmodule Cassandra.Cluster do
  @moduledoc """
  Represents a cassandra cluster. It serves as a Session factory and a collection of metadata.

  It always keeps a control connection open to one of cluster hosts to get notified about
  topological and status changes in the cluster, and keeps its metadata is sync.
  """

  use GenServer

  require Logger

  alias Cassandra.{Connection, ConnectionError, Host, Cache}
  alias Cassandra.Cluster.{Schema, Watcher}

  @defaults [
    fetcher: Schema.Fetcher.V3_0_x,
    cluster_name: nil,
    data_center: nil,
    contact_points: [{127, 0, 0, 1}],
    port: 9042,
    connection_timeout: 1000,
    timeout: 5000,
  ]

  @valid_options Keyword.keys(@defaults) ++ [:retry, :cache]

  @max_tries 5

  ### Client API ###

  @doc """
  Starts a Cluster process without links (outside of a supervision tree).

  See start_link/1 for more information.
  """
  def start(options \\ []) do
    server_options = case Keyword.get(options, :cluster) do
      nil  -> []
      name -> [name: name]
    end
    GenServer.start(__MODULE__, options, server_options)
  end

  @doc """
  Starts a Cluster process linked to the current process.

  `options` is the keyword list of options:

  * `:contact_points` - The initial list host of addresses.  Note that the entire list
      of cluster members will be discovered automatically once a connection to any
      hosts from the original list is successful. (default: `["127.0.0.1"]`)
  * `:port` - Cassandra native protocol port (default: `9042`)
  * `:connection_timeout` - connection timeout in milliseconds (defult: `5000`)
  * `:timeout` - request execution timeout in milliseconds (default: `:infinity`)

  ## Return values

  It returns `{:ok, pid}` when connection to one of `contact_points` established and metadata fetched,
  on any error it returns `{:error, reason}`.
  """
  def start_link(options \\ []) do
    server_options = case Keyword.get(options, :cluster) do
      nil  -> []
      name -> [name: name]
    end
    GenServer.start_link(__MODULE__, options, server_options)
  end

  @doc """
  Returns replications containing `partition_key` of `keyspace`
  """
  def find_replicas(cluster, keyspace, partition_key) do
    GenServer.call(cluster, {:find_replicas, keyspace, partition_key})
  end

  @doc """
  Returns list of `cluster`s `Cassandra.Host`s
  """
  def hosts(cluster) do
    GenServer.call(cluster, :hosts)
  end

  @doc """
  Returns list of `cluster`s up `Cassandra.Host`s
  """
  def up_hosts(cluster) do
    GenServer.call(cluster, :up_hosts)
  end

  @doc """
  Returns list of cluster hosts matching given list if `ips`
  """
  def host(cluster, ips) do
    GenServer.call(cluster, {:host, ips})
  end

  @doc false
  def register(cluster) do
    register(cluster, self())
  end

  @doc false
  def register(cluster, pid) do
    GenServer.call(cluster, {:register, pid})
  end

  ### GenServer Callbacks ###

  @doc false
  def init(options) do
    options =
      @defaults
      |> Keyword.merge(options)
      |> Keyword.take(@valid_options)

    with {socket, supported, local_data} <- select_socket(options),
         {:ok, schema}                   <- Schema.Fetcher.fetch(local_data, socket, options[:fetcher]),
         {:ok, watcher}                  <- Watcher.start_link(options)
    do
      cache =
        case Cache.new(Keyword.get(options, :cache, nil)) do
          {:ok, name} -> name
          :error      -> nil
        end

      initial_state = %{
        cache: cache,
        socket: socket,
        options: options,
        fetcher: options[:fetcher],
        watcher: watcher,
        supported: supported,
        local_data: local_data,
        listeners: [],
      }

      state =
        initial_state
        |> Map.merge(schema)
        |> refresh_schema

      {:ok, state}
    else
      error = %ConnectionError{} -> {:stop, error}
      error = {:error, _reason}  -> {:stop, error}
      :error                     -> {:stop, :no_cache_name}
    end
  end

  @doc false
  def handle_call(:hosts, _from, state) do
    hosts = Map.values(state.hosts)

    {:reply, hosts, state}
  end

  @doc false
  def handle_call(:up_hosts, _from, state) do
    up_hosts =
      state.hosts
      |> Map.values
      |> Enum.filter(&Host.up?/1)

    {:reply, up_hosts, state}
  end

  @doc false
  def handle_call({:host, ips}, _from, state) when is_list(ips) do
    hosts =
      state.hosts
      |> Map.take(ips)
      |> Map.values

    {:reply, hosts, state}
  end

  @doc false
  def handle_call({:host, ip}, _from, state) do
    {:reply, state.hosts[ip], state}
  end

  @doc false
  def handle_call({:find_replicas, keyspace, partition_key}, _from, state) do
    token = state.partitioner.create_token(partition_key)
    replication_token = insertion_point(state.token_ring, token)
    hosts =
      case get_in(state, [:keyspaces, keyspace]) do
        nil      -> []
        keyspace ->
          case List.keyfind(keyspace.replications, replication_token, 0) do
            nil        -> []
            {_, hosts} -> hosts
          end
      end
    {:reply, hosts, state}
  end

  @doc false
  def handle_call({:register, pid}, _from, state) do
    if Process.alive?(pid) and not pid in state.listeners do
      Process.monitor(pid)
      {:reply, :ok, Map.update(state, :listeners, [pid], &[pid | &1])}
    else
      {:reply, :error, state}
    end
  end

  @doc false
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, Map.update(state, :listeners, [], &List.delete(&1, pid))}
  end

  @doc false
  def handle_info({:host, :found, {ip, _}}, state) do
    Logger.info("new host found: #{inspect ip}")
    args = [ip, state.data_center, state.parser]

    state =
      case schema(:fetch_peer, args, state) do
        {{:ok, host}, state} ->
          Enum.each(state.listeners, &send(&1, {:host, :found, host}))
          put_in(state, [:hosts, ip], host)

        _ -> state
      end

    {:noreply, refresh_schema(state)}
  end

  @doc false
  def handle_info({:host, :lost, {ip, _}}, state) do
    Logger.warn("host lost: #{inspect ip}")
    {host, state} = pop_in(state, [:hosts, ip])
    unless is_nil(host) do
      Enum.each(state.listeners, &send(&1, {:host, :lost, host}))
    end
    {:noreply, refresh_schema(state)}
  end

  @doc false
  def handle_info({:host, status, {ip, _}}, state) do
    Logger.info("host #{status} #{inspect ip}")
    {_, state} = get_and_update_in state, [:hosts, ip], fn
      nil  -> :pop
      host ->
        host = Host.toggle(host, status)
        Enum.each(state.listeners, &send(&1, {:host, status, host}))
        {:ok, host}
    end
    {:noreply, state}
  end

  @doc false
  def handle_info({:keyspace, :dropped, keyspace}, state) do
    Logger.info("Keyspace dropped: #{keyspace}")
    {_, state} = pop_in(state, [:keyspaces, keyspace])
    {:noreply, state}
  end

  @doc false
  def handle_info({:keyspace, change, name}, state) do
    Logger.info("Keyspace #{change}: #{name}")
    state =
      case schema(:fetch_keyspace, [name], state) do
        {{:ok, keyspace}, state} ->
          put_in(state, [:keyspaces, name], keyspace)

        _ -> state
      end
    {:noreply, refresh_schema(state)}
  end

  @doc false
  def handle_info(:connected, state) do
    with {{:ok, schema}, state} <- schema(:fetch, [state.local_data], state) do
      state =
        state
        |> Map.merge(schema)
        |> refresh_schema

      {:noreply, state}
    else
      error -> {:stop, error, state}
    end
  end

  @doc false
  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    with {socket, supported, local_data} <- select_socket(state.options) do
      {:noreply, %{state | socket: socket, supported: supported, local_data: local_data}}
    else
      _ -> {:noreply, %{state | socket: nil}}
    end
  end

  @doc false
  def handle_info({:table, _change, {_keyspace, _table}}, state) do
    {:noreply, state}
  end

  @doc false
  def select_socket(options) do
    options
    |> Connection.stream
    |> Stream.flat_map(&fetch_local_data(&1, options))
    |> Enum.take(1)
    |> case do
      [result] -> result
      []       -> ConnectionError.new("select contact point", "not available")
    end
  end

  ### Helpers ###

  defp schema(_, _, %{socket: nil}, @max_tries) do
    ConnectionError.new("reconnection", "faield")
  end

  defp schema(func, args, %{socket: nil} = state, tries) do
    with {socket, supported, local_data} <- select_socket(state.options) do
      schema(func, args, %{state | socket: socket, supported: supported, local_data: local_data}, tries)
    else
      _ -> schema(func, args, state, tries + 1)
    end
  end

  defp schema(func, args, state) do
    case apply(Schema.Fetcher, func, args ++ [state.socket, state.fetcher]) do
      %ConnectionError{reason: :closed} ->
        schema(func, args, %{state | socket: nil}, 0)
      result ->
        {result, state}
    end
  end

  defp refresh_schema(schema) do
    host_tokens = Enum.map(schema.hosts, fn {ip, host} -> {ip, host.tokens} end)
    token_hosts = token_hosts(host_tokens)
    token_ring  = token_ring(host_tokens, token_hosts)
    keyspaces   = put_replications(schema.keyspaces, token_ring, schema)

    Map.merge(schema, %{
      keyspaces: keyspaces,
      token_ring: token_ring,
    })
  end

  defp token_ring(host_tokens, token_hosts) do
    list =
      host_tokens
      |> Keyword.values
      |> Enum.concat
      |> Enum.sort
      |> Enum.uniq

    [head | _] = list
    Enum.map(list ++ [head], &{&1, Map.get(token_hosts, &1)})
  end

  defp token_hosts(host_tokens) do
    host_tokens
    |> Enum.flat_map(fn {ip, tokens} -> Enum.map(tokens, &{&1, ip}) end)
    |> Enum.into(%{})
  end

  defp put_replications(keyspaces, token_ring, schema) do
    keyspaces_with_hash = Enum.map keyspaces, fn {_, keyspace} ->
      {keyspace, :erlang.phash2(keyspace.replication)}
    end

    replications =
      keyspaces_with_hash
      |> Enum.uniq_by(&elem(&1, 1))
      |> Enum.map(fn {keyspace, hash} ->
           reps = Schema.ReplicationStrategy.replications(keyspace.replication, token_ring, schema)
           {hash, reps}
         end)
      |> Enum.into(%{})

    keyspaces_with_hash
    |> Enum.map(fn {keyspace, hash} -> {keyspace.name, Map.put(keyspace, :replications, replications[hash])} end)
    |> Enum.into(%{})
  end

  defp insertion_point([{a,_}], _), do: a
  defp insertion_point([{a,_}, {b,_} | _], item) when a < item and item <= b, do: b
  defp insertion_point([_ | tail], item), do: insertion_point(tail, item)

  defp fetch_local_data({_host, socket, supported}, options) do
    with {:ok, local_data} <- Schema.Fetcher.fetch_local(socket, options[:fetcher]),
         true <- bootstrapped?(local_data),
         true <- in_data_center?(local_data, options[:data_center]),
         true <- named?(local_data, options[:cluster_name])
    do
      [{socket, supported, local_data}]
    else
      _ -> []
    end
  end

  defp bootstrapped?(local_data) do
    Map.get(local_data, "bootstrapped") == "COMPLETED"
  end

  defp named?(_, nil), do: true
  defp named?(local_data, name) do
    Map.get(local_data, "cluster_name") == name
  end

  defp in_data_center?(_, nil), do: true
  defp in_data_center?(local_data, data_center) do
    Map.get(local_data, "data_center") == data_center
  end
end
