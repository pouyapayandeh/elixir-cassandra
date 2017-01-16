defmodule Cassandra.LoadBalancing.TokenAware do
  @moduledoc """
  Token aware balancing policy

  ## Acceptable args

  * `:num_connections` - number of connections to open for each host (default: `1`)
  * `:max_tries` - number of connections to try before on request fail (default: `3`)
  """

  defstruct [
    fallback: %Cassandra.LoadBalancing.RoundRobin{},
    num_connections: 1,
    max_tries: 3,
  ]

  def new(num_connections, max_tries, fallback) do
    options = [
      num_connections: num_connections,
      max_tries: max_tries,
    ]
    struct(__MODULE__, [{:fallback, struct(fallback, options)} | options])
  end

  defimpl Cassandra.LoadBalancing.Policy do
    def plan(balancer, %Cassandra.Statement{keyspace: keyspace, partition_key: partition_key} = statement, schema, connection_manager)
    when not is_nil(keyspace) and not is_nil(partition_key)
    do
      replicas = Cassandra.Cluster.Schema.find_replicas(schema, keyspace, partition_key)
      connections =
        connection_manager
        |> Cassandra.Session.ConnectionManager.connections(replicas)
        |> Cassandra.LoadBalancing.take(balancer.max_tries)

      %{statement | connections: connections}
    end

    def plan(balancer, statement, schema, connection_manager) do
      Cassandra.LoadBalancing.Policy.plan(balancer.fallback, statement, schema, connection_manager)
    end

    def count(balancer, _) do
      balancer.num_connections
    end
  end
end
