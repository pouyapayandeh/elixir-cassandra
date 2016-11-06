defmodule Cassandra.ClusterTest do
  use ExUnit.Case, async: false

  alias Cassandra.Cluster
  import ExUnit.CaptureLog

  @host Cassandra.TestHelper.host

  @tag :capture_log
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
    kill = fn ->
      assert capture_log(fn -> GenServer.stop(conn, :error) end) =~ "control connection lost"
    end
    assert capture_log(kill) =~ "new control connection opened"
    assert conn != :sys.get_state(cluster)[:control_connection]
  end
end
