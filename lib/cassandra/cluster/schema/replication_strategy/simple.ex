defmodule Cassandra.Cluster.Schema.ReplicationStrategy.Simple do
  def replications(replication, token_ring, _schema) do
    size   = Enum.count(token_ring)
    factor = Map.get(replication, "replication_factor", "1")
    factor = case Integer.parse(factor) do
      {x, ""} when size < x -> size
      {factor, ""}          -> factor
      _                     -> 1
    end

    token_ring
    |> Enum.with_index
    |> Enum.map(fn {{token, _host}, i} ->
         hosts =
           token_ring
           |> Stream.cycle
           |> Stream.drop(i)
           |> Stream.take(size)
           |> Stream.map(fn {_, host} -> host end)
           |> Stream.uniq
           |> Enum.take(factor)

         {token, hosts}
       end)
  end
end
