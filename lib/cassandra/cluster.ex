defmodule Cassandra.Cluster do
  @moduledoc """
  Represents a cassandra cluster. It serves as a Session factory and a collection of metadata.

  It always keeps a control connection open to one of cluster hosts to get notified about
  topological and status changes in the cluster, and keeps its metadata is sync.
  """

  use GenServer

  require Logger

  alias Cassandra.Cluster.Schema

  @select_local CQL.encode!(%CQL.Query{query: "SELECT * FROM system.local;"})

  @valid_options [
    :contact_points,
    :port,
    :connection_timeout,
    :timeout,
    :reconnection,
    :retry,
  ]

  ### Client API ###

  @doc """
  Starts a Cluster process without links (outside of a supervision tree).

  See start_link/1 for more information.
  """
  def start(options \\ []) do
    GenServer.start(__MODULE__, options)
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
  * `:reconnection` - {`policy`, `args`} tuple where
      module is an implementation of Cassandra.Reconnection.Policy (defult: `Exponential`)
      args is a list of arguments to pass to `policy` on init (defult: `[]`)
  * `:retry` - {`retry?`, `args`}

  For `gen_server_options` values see `GenServer.start_link/3`.

  ## Return values

  It returns `{:ok, pid}` when connection to one of `contact_points` established and metadata fetched,
  on any error it returns `{:error, reason}`.
  """
  def start_link(options \\ []) do
    GenServer.start(__MODULE__, options)
  end

  def start_session(cluster, options) do
    GenServer.call(cluster, {:start_session, options})
  end

  # def start_session_link(cluster, options) do
  #   with {:ok, session} <- GenServer.call(cluster, {:start_session, options}) do
  #     Process.link(session)
  #     {:ok, session}
  #   end
  # end

  ### GenServer Callbacks ###

  @doc false
  def init(options) do
    options = Keyword.take(options, @valid_options)

    with {:ok, connection, local_data} <- setup(options),
         {:ok, schema} <- Schema.start(local_data, connection)
    do
      {:ok, %{
        options: options,
        schema: schema,
        sessions: [],
      }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_call({:start_session, options}, _from, %{sessions: sessions} = state) do
    pool_name = Keyword.get(options, :pool_name, Cassandra.Session.Executors)
    with {:ok, session} <- Cassandra.Session.Supervisor.start_link(state.schema, pool_name, options) do
      {:reply, {:ok, pool_name}, %{state | sessions: [session | sessions]}}
    else
      error -> {:reply, error, state}
    end
  end

  ### Helpers ###

  defp setup(options) do
    contact_points = Keyword.get(options, :contact_points, ["127.0.0.1"])
    connection_options = Keyword.merge(options, [async_init: false])

    contact_points
    |> Stream.map(&start_connection(&1, connection_options))
    |> Stream.filter_map(&ok?/1, &value/1)
    |> Stream.map(&select_local/1)
    |> Stream.reject(&error?/1)
    |> Stream.filter(&bootstrapped?/1)
    |> Stream.filter(&named?(&1, options[:cluster_name]))
    |> Enum.take(1)
    |> case do
      [{conn, local}] ->
        {:ok, conn, local}
      [] ->
        {:error, :no_avaliable_contact_points}
    end
  end

  defp start_connection({address, port}, options) do
    start_connection(address, Keyword.put(options, :port, port))
  end

  defp start_connection(address, options) do
    options
    |> Keyword.put(:host, address)
    |> Cassandra.Connection.start
  end

  defp ok?({:ok, _}), do: true
  defp ok?(_), do: false

  defp error?(:error), do: true
  defp error?(_), do: false

  defp value({_, v}), do: v

  defp bootstrapped?({_conn, local}) do
    Map.get(local, "bootstrapped") == "COMPLETED"
  end

  def named?(_, nil), do: true
  def named?({_conn, local}, name) do
    Map.get(local, "cluster_name") == name
  end

  defp select_local(conn) do
    with {:ok, rows} <- Cassandra.Connection.send(conn, @select_local),
         [local] <- CQL.Result.Rows.to_map(rows)
    do
      {conn, local}
    else
      _ -> :error
    end
  end
end
