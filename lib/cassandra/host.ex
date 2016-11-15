defmodule Cassandra.Host do
  @moduledoc """
  Represents a Cassandra host
  """

  defstruct [
    :ip,
    :id,
    :data_center,
    :rack,
    :release_version,
    :schema_version,
    :tokens,
    :status,
    :connections,
    :prepared_statements,
  ]

  @doc """
  Creates a new Host struct from given data came from Cassandra `system.peers` or `system.local` tabels
  """
  def new(data, status \\ nil) do
    with {:ok, ip} <- peer_ip(data),
         {:ok, host} <- from_data(data)
    do
      %{host | ip: ip, status: status}
    else
      :error -> nil
    end
  end

  @doc """
  Chacks whether the `host` status is up or not
  """
  def up?({_, host}), do: up?(host)
  def up?(%__MODULE__{} = host), do: host.status == :up

  @doc """
  Chacks whether the `host` status is down or not
  """
  def down?({_, host}), do: down?(host)
  def down?(%__MODULE__{} = host), do: host.status == :down

  @doc """
  Toggles the `host` status to `status`
  """
  def toggle(%__MODULE__{} = host, status)
  when status == :up or status == :down do
    %{host | status: status}
  end

  @doc false
  def toggle_connection(%__MODULE__{} = host, conn, state) do
    put_in(host.connections[conn], state)
  end

  @doc false
  def put_connections(%__MODULE__{} = host, conns, state \\ :close) do
    connections =
      conns
      |> Enum.zip(Stream.cycle([state]))
      |> Enum.into(host.connections)

    put_in(host.connections, connections)
  end

  @doc false
  def delete_connection(%__MODULE__{} = host, conn) do
    update_in(host.connections, &Map.delete(&1, conn))
  end

  @doc false
  def open?(%__MODULE__{} = host, conn) do
    host.connections[conn] == :open
  end

  @doc false
  def open_connections(%__MODULE__{} = host) do
    Enum.filter(host.connections, fn {_, state} -> state == :open end)
  end

  @doc false
  def open_connections_count(%__MODULE__{} = host) do
    host
    |> open_connections
    |> Enum.count
  end

  @doc false
  def put_prepared_statement(%__MODULE__{} = host, hash, prepared) do
    put_in(host.prepared_statements[hash], prepared)
  end

  @doc false
  def delete_prepared_statements(%__MODULE__{} = host) do
    %{host | prepared_statements: %{}}
  end

  @doc false
  def has_prepared?(%__MODULE__{} = host, hash) do
    Map.has_key?(host.prepared_statements, hash)
  end

  defp peer_ip(%{"broadcast_address" => ip}) when not is_nil(ip), do: {:ok, ip}
  defp peer_ip(%{"rpc_address" => {0, 0, 0, 0}, "peer" => peer}), do: {:ok, peer}
  defp peer_ip(%{"rpc_address" => nil, "peer" => peer}), do: {:ok, peer}
  defp peer_ip(%{"rpc_address" => ip}) when not is_nil(ip), do: {:ok, ip}
  defp peer_ip(_), do: :error

  defp from_data(%{
    "host_id" => id,
    "data_center" => data_center,
    "rack" => rack,
    "release_version" => release_version,
    "schema_version" => schema_version,
    "tokens" => tokens,
  }) do
    host = %__MODULE__{
      id: id,
      data_center: data_center,
      rack: rack,
      release_version: release_version,
      schema_version: schema_version,
      tokens: tokens,
      connections: %{},
      prepared_statements: %{},
    }
    {:ok, host}
  end
  defp from_data(_), do: :error
end
