defmodule Cassandra.Session.Worker do
  @moduledoc false

  require Logger

  def send_request(_, from, [], _, _) do
    GenServer.reply(from, {:error, :no_more_connections})
  end

  def send_request(request, from, [conn | conns], retry?, args) do
    Logger.debug("#{__MODULE__} sending request on #{inspect conn}")

    result = Cassandra.Connection.send(conn, request)

    {retry, args} = apply(retry?, [request, result | args])
    if retry do
      send_request(request, from, conns, retry?, args)
    else
      GenServer.reply(from, result)
    end
  end
end
