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
      @task_supervisor __MODULE__.Session.Workers
      @pool_name __MODULE__.Session.Pool

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
          |> Keyword.put(:task_supervisor, @task_supervisor)

        pool_options = [
          name: {:local, @pool_name},
          strategy: Keyword.get(options, :pool_strategy, :lifo),
          size: Keyword.get(options, :pool_size, 10),
          max_overflow: Keyword.get(options, :pool_owerflow, 0),
          worker_module: Session,
        ]

        {contact_points, options} = Keyword.pop(options, :contact_points, ["127.0.0.1"])

        children = [
          worker(Cluster, [contact_points, options, [name: @cluster]]),
          :poolboy.child_spec(Session, pool_options, [@cluster, options]),
          supervisor(Task.Supervisor, [[name: @task_supervisor]])
        ]

        supervise(children, strategy: :rest_for_one)
      end

      def send(request) do
        :poolboy.transaction @pool_name, fn session ->
          Session.send(session, request)
        end
      end

      def prepare(statement) do
        :poolboy.transaction @pool_name, fn session ->
          Session.prepare(session, statement)
        end
      end

      def execute(statement, options \\ []) do
        :poolboy.transaction @pool_name, fn session ->
          Session.execute(session, statement, options)
        end
      end
    end
  end
end
