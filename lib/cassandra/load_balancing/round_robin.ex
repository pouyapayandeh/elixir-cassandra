defmodule Cassandra.LoadBalancing.RoundRobin do
  @moduledoc """
  Round robin load balancing policy

  ## Acceptable args

  * `:num_connections` - number of connections to open for each host (default: `1`)
  * `:max_tries` - number of connections to try before on request fail (default: `3`)
  """

  defstruct [num_connections: 10, max_tries: 3]

  defimpl Cassandra.LoadBalancing.Policy do
    alias Cassandra.LoadBalancing
    alias Cassandra.Session.ConnectionManager

    def plan(balancer, statement, _cluster, connection_manager) do
      connections =
        connection_manager
        |> ConnectionManager.connections
        |> Enum.shuffle
        |> LoadBalancing.take(balancer.max_tries)

      %{statement | connections: connections}
    end

    def count(balancer, _) do
      balancer.num_connections
    end
  end
end
