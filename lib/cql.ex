defmodule CQL do
  @moduledoc false

  def decode(buffer) do
    with {:ok, frame} <- CQL.Frame.decode(buffer) do
      {:ok, decode_body(frame)}
    else
      _ -> CQL.Error.new("unexpected bytes")
    end
  end

  def decode_error(frame) do
    if CQL.Frame.is_error?(frame) do
      with {:ok, %CQL.Frame{body: error}} <- decode(frame) do
        error
      end
    else
      {:ok, frame}
    end
  end

  def decode_body(nil), do: nil

  def decode_body(%CQL.Frame{operation: operation, body: body} = frame) do
    body = case operation do
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
    with {operation, body} <- CQL.Request.encode(request) do
      frame = %CQL.Frame{operation: operation, body: body, stream: stream}
      cql = CQL.Frame.encode(frame)
      {:ok, cql}
    end
  end

  def encode!(request, stream \\ 0) do
    with {:ok, cql} <- encode(request, stream) do
      cql
    else
      error -> raise error
    end
  end

  defdelegate set_stream_id(request, id), to: CQL.Frame
end
