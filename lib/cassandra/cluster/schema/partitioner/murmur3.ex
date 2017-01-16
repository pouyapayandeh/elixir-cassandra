defmodule Cassandra.Cluster.Schema.Partitioner.Murmur3 do
  @behaviour Cassandra.Cluster.Schema.Partitioner

  @long_min -2 |> :math.pow(63) |> trunc
  @long_max  2 |> :math.pow(63) |> trunc |> Kernel.-(1)

  def create_token(partition_key) do
    case Cassandra.Murmur3.x64_128(partition_key) do
      @long_min -> @long_max
      hash      -> hash
    end
  end

  def parse_token(token_string) do
    String.to_integer(token_string)
  end
end
