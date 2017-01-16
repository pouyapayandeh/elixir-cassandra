defmodule CQL do
  @moduledoc false

  def decode(buffer) do
    {frame, rest} = CQL.Frame.decode(buffer)
    {decode_body(frame), rest}
  end

  def decode_body(nil), do: nil

  def decode_body(%CQL.Frame{opration: opration, body: body} = frame) do
    body = case opration do
      :ERROR     -> CQL.Error.decode(body)
      :READY     -> CQL.Ready.decode(body)
      :RESULT    -> CQL.Result.decode(body)
      :SUPPORTED -> CQL.Supported.decode(body)
      :EVENT     -> CQL.Event.decode(body)
      _          -> body
    end

    %CQL.Frame{frame | body: body}
  end

  def encode(request, stream \\ 0) do
    case CQL.Request.encode(request) do
      {opration, body} ->
        frame = %CQL.Frame{opration: opration, body: body, stream: stream}
        cql = CQL.Frame.encode(frame)
        {:ok, cql}
      :error ->
        {:error, :invalid_request}
    end
  end

  def encode!(request, stream \\ 0) do
    case encode(request, stream) do
      {:ok, cql} ->
        cql
      {:error, :invalid_request} ->
        raise ArgumentError.exception("invalid request")
    end
  end

  defdelegate set_stream_id(request, id), to: CQL.Frame
end
