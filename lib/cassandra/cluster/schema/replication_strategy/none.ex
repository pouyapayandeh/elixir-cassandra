defmodule Cassandra.Cluster.Schema.ReplicationStrategy.None do
  def replications(_replications, token_ring, _schema) do
    token_ring
    |> Enum.map(fn {token, host} -> {token, [host]} end)
  end
end
