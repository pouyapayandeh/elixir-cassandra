defmodule Cassandra.ClusterTest do
  use ExUnit.Case

  alias Cassandra.Cluster

  @moduletag :capture_log

  @host Cassandra.TestHelper.host

  test "no_avaliable_contact_points" do
    assert {:error, :no_avaliable_contact_points} = Cluster.start(["127.0.0.1"], [port: 9111])
  end

  test "hosts" do
    assert {:ok, cluster} = Cluster.start_link([@host])
    hosts = Cluster.hosts(cluster)
    assert [%Cassandra.Host{status: :up} | _] = Map.values(hosts)
  end

  test "connection down" do
    assert {:ok, cluster} = Cluster.start_link([@host])
    conn = :sys.get_state(cluster)[:control_connection]
    GenServer.stop(conn, :error)
    assert conn != :sys.get_state(cluster)[:control_connection]
    hosts = Cluster.hosts(cluster)
    assert [%Cassandra.Host{status: :up} | _] = Map.values(hosts)
  end
end
