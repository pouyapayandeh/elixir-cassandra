defmodule Cassandra.Cluster.Schema.Fetcher do
  alias Cassandra.{Connection, Host, Keyspace}
  alias Cassandra.Cluster.Schema

  @type cql :: String.t

  @callback select_keyspaces() :: cql
  @callback select_tables() :: cql
  @callback select_columns() :: cql
  @callback select_indexes() :: cql

  def fetch(local_data, connection, version \\ Schema.Fetcher.V3_0_x) do
    data_center = Map.get(local_data, "data_center")
    cluster_name = Map.get(local_data, "cluster_name")
    partitioner = Schema.Partitioner.partitioner(local_data)
    parser = &partitioner.parse_token/1
    local = Host.new(local_data, :up, data_center, parser)

    with {:ok, peers} <- fetch_peers(data_center, parser, connection, version),
         {:ok, keyspaces} <- fetch_keyspaces(connection, version)
    do
      hosts_map =
        [local | peers]
        |> Enum.map(&{&1.ip, &1})
        |> Enum.into(%{})

      keyspaces_map =
        keyspaces
        |> Enum.map(&{&1.name, &1})
        |> Enum.into(%{})

      schema = %{
        local: local,
        hosts: hosts_map,
        keyspaces: keyspaces_map,
        partitioner: partitioner,
        data_center: data_center,
        cluster_name: cluster_name,
        parser: parser,
      }

      {:ok, schema}
    end
  end

  def fetch_local(connection, version) do
    connection
    |> Connection.query(version.select_local)
    |> one
  end

  def fetch_peers(data_center, parser, connection, version) do
    with %CQL.Result.Rows{} = rows <- Cassandra.Connection.query(connection, version.select_peers) do
      peers =
        rows
        |> CQL.Result.Rows.to_map
        |> Enum.map(&Host.new(&1, :get, data_center, parser))
        |> Enum.reject(&is_nil/1)

      {:ok, peers}
    end
  end

  def fetch_peer(ip, data_center, parser, connection, version) do
    connection
    |> Connection.query(version.select_peer(ip))
    |> one(&Host.new(&1, :down, data_center, parser))
  end

  def fetch_keyspaces(connection, version) do
    with %CQL.Result.Rows{} = rows <- Cassandra.Connection.query(connection, version.select_keyspaces) do
      keyspaces =
        rows
        |> CQL.Result.Rows.to_map
        |> Enum.map(&Keyspace.new/1)
        |> Enum.reject(&is_nil/1)

      {:ok, keyspaces}
    end
  end

  def fetch_keyspace(name, connection, version) do
    connection
    |> Connection.query(version.select_keyspace(name))
    |> one(&Keyspace.new/1)
  end

  defp one(%CQL.Result.Rows{} = rows) do
    case CQL.Result.Rows.to_map(rows) do
      []    -> {:error, :not_found}
      [one] -> {:ok, one}
      [_|_] -> {:error, :many}
    end
  end
  defp one(error), do: error

  defp one(result, mapper) do
    case one(result) do
      {:ok, value} -> {:ok, mapper.(value)}
      error        -> error
    end
  end
end
