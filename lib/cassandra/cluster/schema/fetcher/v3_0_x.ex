defmodule Cassandra.Cluster.Schema.Fetcher.V3_0_x do
  @behaviour Cassandra.Cluster.Schema.Fetcher

  @select_local CQL.encode!(%CQL.Query{query: "SELECT * FROM system.local;"})
  @select_peers CQL.encode!(%CQL.Query{query: "SELECT * FROM system.peers;"})

  def select_local, do: @select_local
  def select_peers, do: @select_peers

  def select_peer(ip) do
    CQL.encode!(%CQL.Query{query: "SELECT * FROM system.peers WHERE peer='#{ip_to_string(ip)}';"})
  end

  def select_schema(name) do
    CQL.encode!(%CQL.Query{query: "SELECT * FROM system_schema.#{name};"})
  end

  def select_keyspace(name) do
    CQL.encode!(%CQL.Query{query: "SELECT * FROM system_schema.keyspaces WHERE keyspace_name='#{name}';"})
  end

  def select_keyspaces, do: select_schema("keyspaces")
  def select_tables,    do: select_schema("tables")
  def select_columns,   do: select_schema("columns")
  def select_indexes,   do: select_schema("indexes")

  defp ip_to_string({_, _, _, _} = ip) do
    ip
    |> Tuple.to_list
    |> Enum.join(".")
  end

  defp ip_to_string({_, _, _, _, _, _} = ip) do
    ip
    |> Tuple.to_list
    |> Enum.map(&Integer.to_string(&1, 16))
    |> Enum.join(":")
  end
end
