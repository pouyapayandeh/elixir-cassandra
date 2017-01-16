defmodule Cassandra.Session.Supervisor do
  use Supervisor

  @default_balancer {Cassandra.LoadBalancing.TokenAware, []}

  def start_link(schema, pool_name, options) do
    Supervisor.start_link(__MODULE__, [schema, pool_name, options])
  end

  def execute(pool, statement) do
    :poolboy.transaction pool, fn executor ->
      Cassandra.Session.Executor.execute(executor, statement)
    end
  end

  def init([schema, pool_name, options]) do
    {balancer_policy, balancer_args} = Keyword.get(options, :balancer, @default_balancer)
    balancer = struct(balancer_policy, balancer_args)

    pool_options = [
      name: {:local, pool_name},
      strategy: Keyword.get(options, :pool_strategy, :lifo),
      size: Keyword.get(options, :pool_size, 10),
      max_overflow: Keyword.get(options, :pool_owerflow, 0),
      worker_module: Cassandra.Session.Executor,
    ]

    task_supervisor = Cassandra.TaskSupervisor
    connection_manager = Cassandra.ConnectionManager

    children = [
      supervisor(Task.Supervisor, [[name: task_supervisor]]),
      worker(Cassandra.Session.ConnectionManager, [schema, balancer, task_supervisor, connection_manager, options]),
      worker(Cassandra.Session.Executor, [schema, balancer, task_supervisor, connection_manager, options]),
      # :poolboy.child_spec(Cassandra.Session.Executor, pool_options, [schema, Cassandra.Session.ConnectionManager, balancer, options]),
    ]

    supervise(children, strategy: :one_for_one)
  end
end
