defmodule Cassandra.ClusterTest do
  use ExUnit.Case

  alias Cassandra.{Cluster, Host, ConnectionError}

  @moduletag :capture_log

  @host Cassandra.TestHelper.host

  setup_all do
    {:ok, cluster} = Cluster.start_link(contact_points: [@host])
    {:ok, %{cluster: cluster}}
  end

  describe "no available contact point" do
    test "#start_link" do
      assert {:error, %ConnectionError{reason: "not available"}} = Cluster.start(port: 9111)
    end
  end

  test "#host", %{cluster: cluster} do
    assert [host = %Host{ip: ip} | _] = Cluster.hosts(cluster)
    assert ^host = Cluster.host(cluster, ip)
    assert [^host] = Cluster.host(cluster, [ip])
  end

  test "#hosts", %{cluster: cluster} do
    assert Enum.all?(Cluster.hosts(cluster), &match?(%Host{}, &1))
  end

  test "#up_hosts", %{cluster: cluster} do
    assert Enum.all?(Cluster.up_hosts(cluster), &match?(%Host{status: :up}, &1))
  end

  test "#find_replicas", %{cluster: cluster} do
    assert [{_, _, _, _}] = Cluster.find_replicas(cluster, "system", "test")
  end
end
