defmodule Cassandra.Session do
  use Supervisor

  @default_balancer {Cassandra.LoadBalancing.TokenAware, []}

  @defaults [
    connection_manager: Cassandra.Session.ConnectionManager,
  ]

  def start_link(cluster, pool_name, options) do
    Supervisor.start_link(__MODULE__, [cluster, pool_name, options])
  end

  def prepare(pool, statement) do
    :poolboy.transaction pool, fn executor ->
      Cassandra.Session.Executor.prepare(executor, statement)
    end
  end

  def execute(pool, statement, values) do
    :poolboy.transaction pool, fn executor ->
      Cassandra.Session.Executor.execute(executor, statement, values)
    end
  end

  def init([cluster, pool_name, options]) do
    {balancer_policy, balancer_args} = Keyword.get(options, :balancer, @default_balancer)
    balancer = struct(balancer_policy, balancer_args)

    options =
      @defaults
      |> Keyword.merge(options)
      |> Keyword.put(:balancer, balancer)

    pool_options = [
      name: {:local, pool_name},
      strategy: Keyword.get(options, :pool_strategy, :lifo),
      size: Keyword.get(options, :pool_size, 10),
      max_overflow: Keyword.get(options, :pool_owerflow, 0),
      worker_module: Cassandra.Session.Executor,
    ]

    children = [
      worker(Cassandra.Session.ConnectionManager, [cluster, options]),
      :poolboy.child_spec(Cassandra.Session.Executor, pool_options, [cluster, options]),
    ]

    supervise(children, strategy: :one_for_one)
  end
end
