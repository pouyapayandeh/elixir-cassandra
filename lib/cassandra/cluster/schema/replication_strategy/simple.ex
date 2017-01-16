defmodule Cassandra.Cluster.Schema.ReplicationStrategy.Simple do
  def replications(replication, token_hosts, token_ring) do
    size = length(token_ring)
    factor = Map.get(replication, "replication_factor", "1")
    factor = case Integer.parse(factor) do
      {x, ""} when size < x -> size
      {factor, ""}          -> factor
      _                     -> 1
    end

    token_ring
    |> Enum.with_index
    |> Enum.map(fn {token, i} ->
         tokens =
           token_ring
           |> Stream.cycle
           |> Stream.drop(i)
           |> Enum.take(factor)

         hosts =
           token_hosts
           |> Keyword.take(tokens)
           |> Keyword.values

         {token, hosts}
       end)
  end
end
