defmodule Cassandra do
  @moduledoc """
  Is a helper to create a session on a Cassandra cluster

  ## Example

  ```elixir
  defmodule Repo do
    use Cassandra, keyspace: "test"
  end

  # Start the repo
  {:ok, _} = Repo.start_link

  # Execute statements
  {:ok, rows} = Repo.execute("SELECT * FROM users;")
  ```
  """

  use Application
  use Supervisor

  alias Cassandra.{Cluster, Session}

  defmacro __using__(opts \\ []) do
    quote do
      use Supervisor

      @cluster            __MODULE__.Cassandra.Cluster
      @session            __MODULE__.Cassandra.Session
      @cache              __MODULE__.Cassandra.Session.Cache
      @connection_manager __MODULE__.Cassandra.Session.ConnectionManager

      def start_link(options \\ []) do
        Supervisor.start_link(__MODULE__, options)
      end

      def init(options) do
        opts = unquote(opts)
        config = case Keyword.fetch(opts, :otp_app) do
          {:ok, app} -> Application.get_env(app, __MODULE__, [])
          :error     -> opts
        end

        options =
          options
          |> Keyword.merge(config)
          |> Keyword.merge([
               cluster: @cluster,
               session: @session,
               cache: @cache,
               connection_manager: @connection_manager,
             ])

        children = [
          worker(Cluster, [options]),
          supervisor(Session, [@cluster, options]),
        ]

        supervise(children, strategy: :rest_for_one)
      end

      def execute(query, options \\ []) do
        Session.execute(@session, query, options)
      end
    end
  end

  ### Application Callbacks ###

  def start(_type, options) do
    Supervisor.start_link(__MODULE__, options)
  end

  ### Supervisor Callbacks ###

  def init(_options) do
    children = [
      worker(Cassandra.UUID, []),
    ]

    supervise(children, strategy: :one_for_one)
  end
end
