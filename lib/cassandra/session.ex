defmodule Cassandra.Session do
  use Supervisor

  alias Cassandra.Session.{Executor, ConnectionManager}

  @default_balancer {Cassandra.LoadBalancing.TokenAware, []}

  @defaults [
    connection_manager: ConnectionManager,
    session: Cassandra.Session,
    pool: DBConnection.Poolboy,
    idle_timeout: 30_000,
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

  def execute(pool, query, options) when is_list(options) do
    timeout = Keyword.get(options, :timeout, :infinity)
    :poolboy.transaction(pool, &Executor.execute(&1, query, options), timeout)
  end

  def init([cluster, options]) do
    {balancer_policy, balancer_args} = Keyword.get(options, :balancer, @default_balancer)
    balancer = balancer_policy.new(balancer_args)

    options =
      @defaults
      |> Keyword.merge(options)
      |> Keyword.put(:balancer, balancer)

    {executor_pool_options, options} = Keyword.pop(options, :executor_pool)
    executor_pool_options = [
      name: {:local, Keyword.fetch!(options, :session)},
      strategy:      Keyword.get(executor_pool_options, :strategy, :lifo),
      size:          Keyword.get(executor_pool_options, :pool_size, 10),
      max_overflow:  Keyword.get(executor_pool_options, :pool_owerflow, 0),
      worker_module: Executor,
    ]

    children = [
      worker(ConnectionManager, [cluster, options]),
      :poolboy.child_spec(Executor, executor_pool_options, [cluster, options]),
    ]

    supervise(children, strategy: :one_for_one)
  end
end
