ExCheck.start
Logger.configure(level: :info)

# Code.require_file("support/cluster_manager.exs", __DIR__)

defmodule Cassandra.TestHelper do
  alias Cassandra.Connection

  @keyspace "elixir_cassandra_test"

  def keyspace, do: @keyspace

  def host do
    (System.get_env("CASSANDRA_CONTACT_POINTS") || "127.0.0.1")
    |> String.split(",")
    |> hd
  end

  def drop_keyspace, do: CQL.encode!(%CQL.Query{query:
    "DROP KEYSPACE IF EXISTS #{@keyspace};"
  })

  def create_keyspace, do: CQL.encode!(%CQL.Query{query: ~s(
    CREATE KEYSPACE #{@keyspace}
      WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
      };
  )})

  def setup do
    {:ok, %{socket: socket}} = Connection.connect(host: host())
    Connection.query(socket, drop_keyspace())
    %CQL.Result.SchemaChange{} = Connection.query(socket, create_keyspace())
    :gen_tcp.close(socket)
  end

  def teardown do
    {:ok, %{socket: socket}} = Connection.connect(host: host())
    %CQL.Result.SchemaChange{} = Connection.query(socket, drop_keyspace())
    :gen_tcp.close(socket)
  end
end

System.at_exit fn _ ->
  Cassandra.TestHelper.teardown
end

Cassandra.TestHelper.setup

ExUnit.start
