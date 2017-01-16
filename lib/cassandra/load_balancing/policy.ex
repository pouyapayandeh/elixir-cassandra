defprotocol Cassandra.LoadBalancing.Policy do
  @moduledoc """
  Protocol to specify a load balancing policy
  """

  def plan(balancer, statement, schema, connections)

  @doc """
  Returns number of connections to open to the `host` where host is a `{ip, data_center, rack}` tuple
  """
  def count(balancer, host)
end
