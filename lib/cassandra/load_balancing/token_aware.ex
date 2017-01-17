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
    alias Cassandra.{Statement, LoadBalancing}
    alias Cassandra.Cluster.Schema
    alias Cassandra.Session.ConnectionManager

    def plan(balancer, %Statement{keyspace: keyspace, partition_key: partition_key} = statement, schema, connection_manager)
    when not is_nil(keyspace) and not is_nil(partition_key)
    do
      case Schema.find_replicas(schema, keyspace, partition_key) do
        [] ->
          {:error, :keyspace_not_found}
        replicas ->
          connections =
            connection_manager
            |> ConnectionManager.connections(replicas)
            |> LoadBalancing.take(balancer.max_tries)

          {:ok, %{statement | connections: connections}}
      end
    end

    def plan(balancer, statement, schema, connection_manager) do
      LoadBalancing.Policy.plan(balancer.fallback, statement, schema, connection_manager)
    end

    def count(balancer, _) do
      balancer.num_connections
    end
  end
end
