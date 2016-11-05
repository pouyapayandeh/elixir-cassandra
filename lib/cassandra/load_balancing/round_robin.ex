defmodule Cassandra.LoadBalancing.RoundRobin do
  @moduledoc """
  Round robin load balancing policy

  ## Acceptable args

  * `:num_connections` - number of connections to open for each host (default: `2`)
  """

  alias Cassandra.Host

  defstruct [num_connections: 2]

  defimpl Cassandra.LoadBalancing.Policy do
    def select(_, hosts, _) do
      hosts
      |> Enum.flat_map(&Host.open_connections(&1))
      |> Enum.shuffle
    end

    def count(balancer, _) do
      balancer.num_connections
    end
  end
end
