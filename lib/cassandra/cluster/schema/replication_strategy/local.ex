defmodule Cassandra.Cluster.Schema.ReplicationStrategy.Local do
  def replications(_replications, token_ring, schema) do
    Enum.map(token_ring, fn {token, _host} -> {token, [schema.local.ip]} end)
  end
end
