defmodule Cassandra.Cluster.Schema.ReplicationStrategy do
  alias Cassandra.Cluster.Schema.ReplicationStrategy.{Simple, Local, None}

  @default_strategy None
  @strategies %{
    "org.apache.cassandra.locator.SimpleStrategy" => Simple,
    "org.apache.cassandra.locator.LocalStrategy"  => Local,
  }

  def replications(replication, token_ring, schema) do
    strategy(replication).replications(replication, token_ring, schema)
  end

  def strategy(replication) do
    class = Map.get(replication, "class")
    Map.get(@strategies, class, @default_strategy)
  end
end
