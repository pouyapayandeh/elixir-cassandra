defmodule Cassandra.Session do
  use Supervisor

  alias Cassandra.Statement

  @default_balancer {Cassandra.LoadBalancing.TokenAware, []}

  @defaults [
    connection_manager: Cassandra.Session.ConnectionManager,
    session: Cassandra.Session,
    pool: DBConnection.Poolboy,
    queue: false,
    executor_pool: [
      size: 10,
      owerflow_size: 0,
      strategy: :lifo,
    ],
  ]

  def start_link(cluster, options \\ []) do
    Supervisor.start_link(__MODULE__, [cluster, options])
  end

  def execute(pool, query, options \\ [])

  def execute(pool, %Statement{} = statement, values) do
    :poolboy.transaction pool, fn executor ->
      Cassandra.Session.Executor.execute(executor, statement, values)
    end
  end

  def execute(pool, query, options) do
    execute(pool, Statement.new(query, options), Keyword.get(options, :values, []))
  end

  def init([cluster, options]) do
    {balancer_policy, balancer_args} = Keyword.get(options, :balancer, @default_balancer)
    balancer = struct(balancer_policy, balancer_args)

    options =
      @defaults
      |> Keyword.merge(options)
      |> Keyword.put(:balancer, balancer)

    {executor_pool_options, options} = Keyword.pop(options, :executor_pool)
    executor_pool_options = [
      name: {:local, Keyword .fetch!(options, :session)},
      strategy:      Keyword .get(executor_pool_options, :strategy, :lifo),
      size:          Keyword .get(executor_pool_options, :pool_size, 10),
      max_overflow:  Keyword .get(executor_pool_options, :pool_owerflow, 0),
      worker_module: Cassandra.Session.Executor,
    ]

    children = [
      worker(Cassandra.Session.ConnectionManager, [cluster, options]),
      :poolboy.child_spec(Cassandra.Session.Executor, executor_pool_options, [cluster, options]),
    ]

    supervise(children, strategy: :one_for_one)
  end
end
