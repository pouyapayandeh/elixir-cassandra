defmodule Cassandra.LoadBalancing.DCAware do
  @moduledoc """
  DC Aware Round robin load balancing policy

  ## Acceptable args

  * `:num_connections` - number of connections to open for each host (default: `1`)
  * `:max_tries` - number of connections to try before on request fail (default: `3`)
  * `:dc' - prefered dc to get data from
  """

  defstruct [num_connections: 10, max_tries: 3, dc: "dc1"]

  def new(args) do
    struct(__MODULE__, args)
  end

  defimpl Cassandra.LoadBalancing.Policy do
    alias Cassandra.LoadBalancing
    alias Cassandra.Session.ConnectionManager
    alias Cassandra.Cluster

    @spec plan(any, any, atom | pid | {atom, any} | {:via, atom, any}, any) ::
            {:error, :no_hosts} | %{connections: [any]}
    def plan(balancer, statement, cluster, connection_manager) do
      case Cluster.hosts(cluster) do
        [] ->{:error, :no_hosts}
        hosts ->
          dc_hosts = Enum.filter(hosts, fn host -> Map.get(host, :data_center) == balancer.dc  end) |> Enum.map(fn host -> Map.get(host, :ip) end)
          connections =
            connection_manager
            |> ConnectionManager.connections(dc_hosts)
            |> Enum.shuffle
            |> LoadBalancing.take(balancer.max_tries)
          %{statement | connections: connections}
        end
    end

    def count(balancer, _) do
      balancer.num_connections
    end
  end
end
