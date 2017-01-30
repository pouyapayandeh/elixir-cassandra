defmodule Cassandra.Cluster.Schema.ReplicationStrategy.None do
  def replications(_replications, token_ring, _schema) do
    Enum.map(token_ring, fn {token, host} -> {token, [host]} end)
  end
end
