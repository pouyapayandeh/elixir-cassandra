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

  alias Cassandra.{Cluster, Session}

  defmacro __using__(opts \\ []) do
    quote do
      use Supervisor

      @cluster __MODULE__.Cluster
      @session __MODULE__.Session
      @schema __MODULE__.Cluster.Schema
      @task_supervisor __MODULE__.Task.Supervisor

      def start_link(options \\ []) do
        Supervisor.start_link(__MODULE__, options)
      end

      def init(options) do
        opts = unquote(opts)
        config = case Keyword.fetch(opts, :otp_app) do
          {:ok, app} ->
            Application.get_env(app, __MODULE__, [])
          :error ->
            opts
        end

        options =
          options
          |> Keyword.merge(config)
          |> Keyword.merge([
               schema_name: @schema,
               task_supervisor: @task_supervisor,
             ])

        children = [
          worker(Cluster, [Keyword.put(options, :name, @cluster)]),
          supervisor(Session, [@schema, @session, options]),
        ]

        supervise(children, strategy: :rest_for_one)
      end


      def execute(statement) do
        :poolboy.transaction @session, fn session ->
          Session.execute(session, statement)
        end
      end

    end
  end
end
