defmodule Cassandra.Cluster.Schema.Fetcher do
  alias Cassandra.{Host, Keyspace}
  alias Cassandra.Cluster.Schema

  @type cql :: String.t

  @callback select_keyspaces() :: cql
  @callback select_tables() :: cql
  @callback select_columns() :: cql
  @callback select_indexes() :: cql

  def fetch(data, connection, version \\ Schema.Fetcher.V3_0_x) do
    data_center = Map.get(data, "data_center")
    partitioner = Schema.Partitioner.partitioner(data)

    parser = &partitioner.parse_token/1
    local  = Host.new(data, :up, data_center, parser)
    peers  = fetch_peers(data_center, parser, connection, version)
    hosts  = Enum.map([local | peers], &{&1.ip, &1}) |> Enum.into(%{})

    keyspaces = fetch_keyspaces(connection, version)

    %{
      hosts: hosts,
      keyspaces: keyspaces,
      partitioner: partitioner,
    }
  end

  def fetch_local(connection, version) do
    with {:ok, [local]} <- Cassandra.Connection.send(connection, version.select_peers) do
      local
    else
      _ -> %{}
    end
  end

  def fetch_peer(ip, connection, version) do
    with {:ok, [peer]} <- Cassandra.Connection.send(connection, version.select_peer(ip)) do
      peer
    else
      _ -> %{}
    end
  end

  def fetch_keyspace(name, connection, version) do
    with {:ok, result} <- Cassandra.Connection.send(connection, version.select_keyspace(name)),
         [keyspace] <- CQL.Result.Rows.to_map(result)
    do
      Keyspace.new(keyspace)
    end
  end

  def fetch_peers(data_center, parser, connection, version) do
    with {:ok, peers} <- Cassandra.Connection.send(connection, version.select_peers) do
      peers
      |> CQL.Result.Rows.to_map
      |> Enum.map(&Host.new(&1, :up, data_center, parser))
      |> Enum.reject(&is_nil/1)
    end
  end

  def fetch_keyspaces(connection, version) do
    with {:ok, keyspaces} <- Cassandra.Connection.send(connection, version.select_keyspaces) do
      keyspaces
      |> CQL.Result.Rows.to_map
      |> Enum.map(&Keyspace.new/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&{&1.name, &1})
      |> Enum.into(%{})
    end
  end
end
