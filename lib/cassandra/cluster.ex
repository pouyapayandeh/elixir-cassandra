defmodule Cassandra.Cluster do
  @moduledoc """
  Represents a cassandra cluster. It serves as a Session factory and a collection of metadata.

  It always keeps a control connection open to one of cluster hosts to get notified about
  topological and status changes in the cluster, and keeps its metadata is sync.
  """

  use GenServer

  require Logger

  alias Cassandra.Connection
  alias Cassandra.Cluster.Schema

  @default_fetcher Schema.Fetcher.V3_0_x
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
    server_options = Keyword.take(options, [:name])
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
  * `:reconnection` - {`policy`, `args`} tuple where
      module is an implementation of Cassandra.Reconnection.Policy (defult: `Exponential`)
      args is a list of arguments to pass to `policy` on init (defult: `[]`)
  * `:retry` - {`retry?`, `args`}

  ## Return values

  It returns `{:ok, pid}` when connection to one of `contact_points` established and metadata fetched,
  on any error it returns `{:error, reason}`.
  """
  def start_link(options \\ []) do
    server_options = Keyword.take(options, [:name])
    GenServer.start(__MODULE__, options, server_options)
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
    fetcher = Keyword.get(options, :fetcher, @default_fetcher)
    schema_name = Keyword.get(options, :schema_name)
    options = Keyword.take(options, @valid_options)

    with {:ok, local_data, connection} <- setup(fetcher, options),
         {:ok, schema} <- Schema.start_link(local_data, connection, fetcher, name: schema_name)
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
    with {:ok, session} <- Cassandra.Session.start_link(state.schema, pool_name, options) do
      {:reply, {:ok, pool_name}, %{state | sessions: [session | sessions]}}
    else
      error -> {:reply, error, state}
    end
  end

  ### Helpers ###

  defp setup(fetcher, options) do
    contact_points = Keyword.get(options, :contact_points, ["127.0.0.1"])
    connection_options = Keyword.merge(options, [async_init: false])

    contact_points
    |> Stream.map(&setup_connection(&1, connection_options, fetcher))
    |> Stream.reject(&error?/1)
    |> Enum.take(1)
    |> case do
      [result] -> result
      []       -> {:error, :no_contact_point_available}
    end
  end

  defp setup_connection(address, options, fetcher) do
    with {:ok, connection} <- Connection.start(address, options),
         {:ok, local_data} <- Schema.Fetcher.fetch_local(connection, fetcher)
    do
      if bootstrapped?(local_data) and named?(local_data, options[:cluster_name]) do
        {:ok, local_data, connection}
      else
        :error
      end
    end
  end

  defp error?(:error), do: true
  defp error?(_), do: false

  defp bootstrapped?(local_data) do
    Map.get(local_data, "bootstrapped") == "COMPLETED"
  end

  def named?(_, nil), do: true
  def named?(local_data, name) do
    Map.get(local_data, "cluster_name") == name
  end
end
