defmodule Cassandra.Cluster.Schema do
  use GenServer

  require Logger

  alias Cassandra.Host
  alias Cassandra.Cluster.Schema

  @register_events CQL.encode!(%CQL.Register{})
  @default_fetcher Schema.Fetcher.V3_0_x

  ### API ###

  def start(data, connection, fetcher \\ @default_fetcher) do
    GenServer.start(__MODULE__, [data, connection, fetcher])
  end

  def register(schema, pid) do
    GenServer.cast(schema, {:register, pid})
  end

  def find_replicas(schema, keyspace, partition_key) do
    GenServer.call(schema, {:find_replicas, keyspace, partition_key})
  end

  def up_hosts(schema) do
    GenServer.call(schema, :up_hosts)
  end

  def host(schema, ip) do
    GenServer.call(schema, {:host, ip})
  end

  ### GenServer Callbacks ###

  def init([data, connection, fetcher]) do
    schema = Schema.Fetcher.fetch(data, connection, fetcher)

    host_tokens = Enum.map(schema.hosts, fn {ip, host} -> {ip, host.tokens} end)
    token_hosts = token_hosts(host_tokens)
    token_ring  = token_ring(host_tokens)
    keyspaces   = put_replications(schema.keyspaces, token_ring, token_hosts)

    state = Map.merge(schema, %{
      token_ring: token_ring,
      keyspaces: keyspaces,
      connection: connection,
      listeners: [],
      fetcher: fetcher,
    })

    with :ok <- register_events(connection) do
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason, %{}}
    end
  end

  def token_ring(host_tokens) do
    list =
      host_tokens
      |> Keyword.values
      |> Enum.concat
      |> Enum.sort
      |> Enum.uniq

    [head | _] = list
    list ++ [head]
  end

  def token_hosts(host_tokens) do
    host_tokens
    |> Enum.flat_map(fn {ip, tokens} -> Enum.map(tokens, &{&1, ip}) end)
  end

  def put_replications(keyspaces, token_ring, token_hosts) do
    keyspaces_with_hash = Enum.map keyspaces, fn {_, keyspace} ->
      {keyspace, :erlang.phash2(keyspace.replication)}
    end

    replications =
      keyspaces_with_hash
      |> Enum.uniq_by(&elem(&1, 1))
      |> Enum.map(fn {keyspace, hash} ->
           reps = Schema.ReplicationStrategy.replications(keyspace.replication, token_hosts, token_ring)
           {hash, reps}
         end)
      |> Enum.into(%{})

    keyspaces_with_hash
    |> Enum.map(fn {keyspace, hash} -> {keyspace.name, Map.put(keyspace, :replications, replications[hash])} end)
    |> Enum.into(%{})
  end

  @doc false
  def handle_cast({:register, pid}, %{listeners: listeners} = state) do
    {:noreply, %{state | listeners: [pid | listeners]}}
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
  def handle_info({:event, _event}, %{owned: false} = state) do
    Logger.warn("Missing schema events due to not owning ets table")
    {:noreply, state}
  end

  @doc false
  def handle_info({:event, event}, %{listeners: listeners} = state) do
    event = event_type(event)
    Enum.each(listeners, &send(&1, {:event, event}))
    handle_event(event, state)
  end

  ### Helpers ###

  defp handle_event({:host, :found, {ip, _}}, state) do
    Logger.info("#{__MODULE__} new host found: #{inspect ip}")

    host =
      ip
      |> Schema.Fetcher.fetch_peer(state.connection, state.fetcher)
      |> Host.new(:up, state.data_center, &state.partitioner.parse_token/1)

    state = put_in(state, [:hosts, ip], host)

    {:noreply, state}
  end

  defp handle_event({:host, :lost, {ip, _}}, state) do
    Logger.warn("#{__MODULE__} host lost: #{inspect ip}")
    {_, state} = pop_in(state, [:hosts, ip])
    {:noreply, state}
  end

  defp handle_event({:host, status, {ip, _}}, state) do
    Logger.info("#{__MODULE__} host #{status} #{inspect ip}")
    state = put_in(state, [:hosts, ip, :status], status)
    {:noreply, state}
  end

  defp handle_event({:keyspace, :dropped, keyspace}, state) do
    Logger.info("Keyspace dropped: #{keyspace}")
    {_, state} = pop_in(state, [:keyspaces, keyspace])
    {:noreply, state}
  end

  defp handle_event({:keyspace, _, name}, state) do
    case Schema.Fetcher.fetch_keyspace(name, state.connection, state.fetcher) do
      nil ->
        {:noreply, state}
      keyspace ->
        state = put_in(state, [:keyspaces, name], keyspace)
        {:noreply, state}
    end
  end

  defp handle_event(event, state) do
    Logger.debug("Unhandled event in schema #{inspect event}")
    {:noreply, state}
  end

  defp register_events(connection) do
    with :ok <- Cassandra.Connection.manage_events(connection),
         {:ok, :ready} <- Cassandra.Connection.send(connection, @register_events)
    do
      Process.link(connection)
      :ok
    end
  end

  defp event_type(%CQL.Event{type: type, info: %{change: change, address: address}})
  when type in ["TOPOLOGY_CHANGE", "STATUS_CHANGE"]
  do
    change =
      case change do
        "NEW_NODE"     -> :found
        "REMOVED_NODE" -> :lost
        "UP"           -> :up
        "DOWN"         -> :down
      end
    {:host, change, address}
  end

  defp event_type(%CQL.Event{type: "SCHEMA_CHANGE", info: %{change: change, target: "KEYSPACE", options: %{keyspace: keyspace}}}) do
    change =
      case change do
        "CREATED" -> :created
        "UPDATED" -> :updated
        "DROPPED" -> :dropped
      end
    {:keyspace, change, keyspace}
  end

  defp event_type(event) do
    event
  end

  defp insertion_point([a], _), do: a
  defp insertion_point([a, b | _], item) when a < item and item < b, do: b
  defp insertion_point([_ | tail], item), do: insertion_point(tail, item)
end
