defmodule Cassandra.Session.Worker do
  @moduledoc false

  require Logger

  def send_request(request, from, conns, retry) do
    profile = %{start_time: :erlang.monotonic_time}
    send_request(request, from, conns, retry, profile)
  end

  def send_request(_, from, [], _, profile) do
    queue_time = :erlang.monotonic_time - profile.start_time
    GenServer.reply from,
      profile
      |> Map.update(:queue_times, [queue_time], &[queue_time | &1])
      |> Map.put(:result, {:error, :no_more_connections})
  end

  def send_request(request, from, [conn | conns], {retry?, args}, profile) do
    queue_time = :erlang.monotonic_time - profile.start_time
    {query_time, result} = :timer.tc(Cassandra.Connection, :send, [conn, request, :infinity])

    profile =
      profile
      |> Map.update(:queue_times, [queue_time], &[queue_time | &1])
      |> Map.update(:query_times, [query_time], &[query_time | &1])
      |> Map.update(:connections, [conn], &[conn | &1])

    {retry, args} = apply(retry?, [request, result | args])
    if retry do
      send_request(request, from, conns, {retry?, args}, profile)
    else
      GenServer.reply(from, Map.put(profile, :result, result))
    end
  end
end
