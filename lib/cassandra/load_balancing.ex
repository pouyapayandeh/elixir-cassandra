defmodule Cassandra.LoadBalancing do
  @moduledoc false

  alias Cassandra.LoadBalancing.Policy
  alias Cassandra.Session.ConnectionManager

  @distances [:ignore, :local, :remote]

  defmacro distances, do: @distances

  def plan(statement, balancer, cluster, connection_manager) do
    if Keyword.get(statement.options, :on_coordinator, false) do
      %{statement | connections: ConnectionManager.connections(connection_manager)}
    else
      Policy.plan(balancer, statement, cluster, connection_manager)
    end
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
