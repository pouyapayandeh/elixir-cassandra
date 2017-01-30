defmodule Cassandra.Session.ConnectionManager do
  use GenServer

  alias Cassandra.{Cluster, Connection, LoadBalancing}

  ### API ###

  def start_link(cluster, options) do
    with {:ok, balancer} <- Keyword.fetch(options, :balancer) do
      server_options = case Keyword.get(options, :connection_manager) do
        nil  -> []
        name -> [name: name]
      end
      GenServer.start_link(__MODULE__, [cluster, balancer, options], server_options)
    else
      :error -> {:error, :missing_balancer}
    end
  end

  def connections(manager) do
    GenServer.call(manager, :connections)
  end

  def connections(manager, ip_list) do
    GenServer.call(manager, {:connections, ip_list})
  end

  ### GenServer Callbacks ###

  def init([cluster, balancer, options]) do
    connections =
      cluster
      |> Cluster.up_hosts
      |> Enum.map(&start_connection(&1, balancer, options))
      |> Enum.filter_map(&match?({:ok, _, _}, &1), fn {:ok, ip, pid} -> {ip, pid} end)

    state = %{
      cluster: cluster,
      balancer: balancer,
      options: options,
      connections: connections,
    }

    {:ok, state}
  end

  def handle_call(:connections, _from, %{connections: connections} = state) do
    {:reply, connections, state}
  end

  def handle_call({:connections, ips}, _from, %{connections: connections} = state) do
    ips = ips |> List.wrap |> MapSet.new
    reply = Enum.filter(connections, fn {ip, _pid} -> ip in ips end)

    {:reply, reply, state}
  end

  def handle_info({:host, :up, host}, state) do
    connections =
      case start_connection(host, state.balancer, state.options) do
        {:ok, ip, pid} -> [{ip, pid} | state.connections]
        _              -> state.connections
      end

    {:noreply, %{state | connections: connections}}
  end

  def handle_info({:host, status, host}, state)
  when status in [:down, :lost]
  do
    connections =
      case List.keyfind(state.connections, host.ip, 0) do
        nil -> state.connections
        {_, pid} = item ->
          GenServer.stop(pid)
          List.delete(state.connections, item)
      end
    {:noreply, %{state | connections: connections}}
  end

  def handle_info({:host, _status, _host}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    connections = List.keydelete(state.connections, pid, 1)
    {:noreply, %{state | connections: connections}}
  end

  ### Helpers ###

  defp connection_options(host, count, options) do
    Keyword.merge(options, [
      host: host.ip,
      pool_size: count,
    ])
  end

  defp start_connection(host, balancer, options) do
    count = LoadBalancing.count(balancer, host)
    with {:ok, pid} <- DBConnection.start_link(Connection, connection_options(host, count, options)) do
      Process.monitor(pid)
      Process.unlink(pid)
      {:ok, host.ip, pid}
    end
  end
end
