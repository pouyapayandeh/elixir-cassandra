defmodule Cassandra.Cluster.Schema.Partitioner do
  @type partition_key :: term
  @type token :: term

  @callback create_token(partition_key) :: token
  @callback parse_token(String.t) :: token

  def partitioner(%{"partitioner" => "org.apache.cassandra.dht.Murmur3Partitioner"}) do
    Cassandra.Cluster.Schema.Partitioner.Murmur3
  end

  def partitioner(_) do
    nil
  end
end
