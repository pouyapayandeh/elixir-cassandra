defmodule Cassandra.Cluster.Schema.ReplicationStrategy.None do
  def replications(_replications, token_hosts, _token_ring) do
    token_hosts
    |> Enum.map(fn {token, host} -> {token, [host]} end)
  end
end
