defprotocol Cassandra.LoadBalancing.Policy do
  @moduledoc """
  Protocol to specify a load balancing policy
  """

  @doc """
  Selects from a list of `connections` for the `request`

  Returns a sorted list of `connections` to use for executing the `request`
  """
  def select(balancer, connections, request)

  @doc """
  Returns number of connections to open to the `host`
  """
  def count(balancer, host)
end
