defmodule Cassandra.Keyspace do
  defstruct [:name, durable_writes: true, replication: %{}, tables: %{}, replications: %{}]

  def new(%{
    "keyspace_name" => name,
    "durable_writes" => durable_writes,
    "replication" => replication,
  })
  do
    %__MODULE__{
      name: name,
      durable_writes: durable_writes,
      replication: replication,
    }
  end
  def new(_), do: nil
end
