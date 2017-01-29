defmodule Cassandra.LoadBalancing do
  @moduledoc false

  alias Cassandra.LoadBalancing.Policy

  @distances [:ignore, :local, :remote]

  defmacro distances, do: @distances

  def plan(statement, balancer, cluster, connection_manager) do
    Policy.plan(balancer, statement, cluster, connection_manager)
  end

  def count(balancer, host) do
    Policy.count(balancer, host)
  end

  def take([], _), do: []
  def take(list, n) do
    list
    |> Stream.cycle
    |> Enum.take(n)
  end
end
