defmodule Cassandra.LoadBalancing.TokenAware do
  @moduledoc """
  Token aware balancing policy

  ## Acceptable args

  * `:num_connections` - number of connections to open for each host (default: `1`)
  * `:max_tries` - number of connections to try before on request fail (default: `3`)
  """

  defstruct [
    wrapped: %Cassandra.LoadBalancing.RoundRobin{},
  ]

  defimpl Cassandra.LoadBalancing.Policy do
    alias Cassandra.{Cluster, Statement, LoadBalancing}
    alias Cassandra.Session.ConnectionManager

    def plan(balancer, %Statement{keyspace: keyspace, partition_key: partition_key} = statement, cluster, connection_manager)
    when not is_nil(keyspace) and not is_nil(partition_key)
    do
      case Cluster.find_replicas(cluster, keyspace, partition_key) do
        [] ->
          {:error, :keyspace_not_found}
        replicas ->
          connections =
            connection_manager
            |> ConnectionManager.connections(replicas)
            |> LoadBalancing.take(balancer.wrapped.max_tries)

          %Statement{statement | connections: connections}
      end
    end

    def plan(balancer, statement, cluster, connection_manager) do
      LoadBalancing.Policy.plan(balancer.wrapped, statement, cluster, connection_manager)
    end

    def count(balancer, host) do
      LoadBalancing.Policy.count(balancer.wrapped, host)
    end
  end
end
