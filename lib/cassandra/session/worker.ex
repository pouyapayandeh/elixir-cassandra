defmodule Cassandra.Session.Worker do
  @moduledoc false

  require Logger

  def send_request(request, from, conns, retry) do
    send_request(request, from, conns, retry, :erlang.monotonic_time)
  end

  def send_request(_, from, [], _, start_time) do
    queue_time = :erlang.monotonic_time - start_time
    GenServer.reply(from, %{result: {:error, :no_more_connections}, queue_time: queue_time})
  end

  def send_request(request, from, [conn | conns], {retry?, args}, start_time) do
    queue_time = :erlang.monotonic_time - start_time
    {query_time, result} = :timer.tc(Cassandra.Connection, :send, [conn, request, :infinity])
    {retry, args} = apply(retry?, [request, result | args])
    if retry do
      send_request(request, from, conns, {retry?, args}, start_time)
    else
      GenServer.reply(from, %{result: result, queue_time: queue_time, query_time: query_time, connection_pid: conn})
    end
  end
end
