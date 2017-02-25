defmodule Cassandra.Cluster.Watcher do
  use GenServer

  require Logger

  alias Cassandra.{Cluster, Connection, ConnectionError}

  @initial_backoff 1000
  @max_backoff 12_000
  @register_events CQL.encode!(%CQL.Register{})

  def start_link(options) do
    with {:ok, pid} <- GenServer.start_link(__MODULE__, options, server_options(options)),
         :ok        <- register(pid)
    do
      {:ok, pid}
    end
  end

  def register(watcher) do
    GenServer.call(watcher, {:register, self()})
  end

  def register(watcher, pid) do
    GenServer.call(watcher, {:register, pid})
  end

  ### GenServer Callbacks ###

  def init(options) do
    backoff = @initial_backoff
    with {:ok, socket} <- setup(options) do
      {:ok, %{socket: socket, options: options, listeners: [], backoff: backoff}}
    else
      %ConnectionError{} ->
        Process.send_after(self(), :connect, backoff)
        {:ok, %{socket: nil, options: options, listeners: [], backoff: next_backoff(backoff)}}
    end
  end

  def handle_call({:register, pid}, _from, state) do
    if Process.alive?(pid) and not pid in state.listeners do
      Process.monitor(pid)
      {:reply, :ok, Map.update(state, :listeners, [pid], &[pid | &1])}
    else
      {:reply, :error, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, Map.update(state, :listeners, [], &List.delete(&1, pid))}
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    with {:ok, %CQL.Frame{body: cql_event}} <- CQL.decode(data) do
      case event_type(cql_event) do
        :error -> :ok
        event  -> Enum.each(state.listeners, &send(&1, event))
      end
    end
    {:noreply, state}
  end

  def handle_info(:connect, %{socket: nil, backoff: backoff} = state) do
    Logger.info("Cassandra watcher tring to connect ...")
    with {:ok, socket} <- setup(state.options) do
      Logger.info("Cassandra watcher connected")
      Enum.each(state.listeners, &send(&1, :connected))
      {:noreply, %{state | socket: socket, backoff: @initial_backoff}}
    else
      %ConnectionError{} ->
        Process.send_after(self(), :connect, backoff)
        {:noreply, %{state | socket: nil, backoff: next_backoff(backoff)}}
    end
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    Logger.warn("Cassandra watcher connection lost")
    handle_info(:connect, %{state | socket: nil})
  end

  ###

  defp setup(options) do
    with {socket, _, _} <- Cluster.select_socket(options),
         :ok            <- register_events(socket),
         :ok            <- :inet.setopts(socket, active: true)
    do
      {:ok, socket}
    end
  end

  def register_events(socket) do
    socket
    |> Connection.query(@register_events)
    |> ready?
  end

  def ready?(%CQL.Ready{}), do: :ok
  def ready?(error), do: error

  defp next_backoff(n) do
    m = min(n * 1.2, @max_backoff)
    trunc((0.2 * (:rand.uniform + 1) * m) + m)
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

  defp event_type(%CQL.Event{type: type, info: %{change: change, target: target, options: %{keyspace: keyspace}}})
  when type == "SCHEMA_CHANGE" and target == "KEYSPACE"
  do
    change =
      case change do
        "CREATED" -> :created
        "UPDATED" -> :updated
        "DROPPED" -> :dropped
      end
    {:keyspace, change, keyspace}
  end

  defp event_type(%CQL.Event{type: type, info: %{change: change, target: target, options: %{keyspace: keyspace, table: table}}})
  when type == "SCHEMA_CHANGE" and target == "TABLE"
  do
    change =
      case change do
        "CREATED" -> :created
        "UPDATED" -> :updated
        "DROPPED" -> :dropped
      end
    {:table, change, {keyspace, table}}
  end

  defp event_type(_event), do: :error

  defp server_options(options) do
    case Keyword.get(options, :watcher) do
      nil  -> []
      name -> [name: name]
    end
  end
end
