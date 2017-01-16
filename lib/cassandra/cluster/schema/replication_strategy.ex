defmodule Cassandra.Cluster.Schema.ReplicationStrategy do
  alias Cassandra.Cluster.Schema.ReplicationStrategy

  @default_strategy ReplicationStrategy.None
  @strategies %{
    "org.apache.cassandra.locator.SimpleStrategy" => ReplicationStrategy.Simple,
  }

  def replications(replication, token_hosts, token_ring) do
    strategy(replication).replications(replication, token_hosts, token_ring)
  end

  def strategy(replication) do
    class = Map.get(replication, "class")
    Map.get(@strategies, class, @default_strategy)
  end
end
