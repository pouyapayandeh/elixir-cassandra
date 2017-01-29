defmodule CQL.Frame do
  @moduledoc false

  import CQL.DataTypes.Encoder
  alias CQL.DataTypes.Decoder

  defstruct [
    version: 0x04,
    flags: [],
    stream: 0,
    operation: 0,
    length: 0,
    warnings: [],
    tracing_id: nil,
    body: "",
  ]

  @operations %{
    :ERROR          => 0x00,
    :STARTUP        => 0x01,
    :READY          => 0x02,
    :AUTHENTICATE   => 0x03,
    :OPTIONS        => 0x05,
    :SUPPORTED      => 0x06,
    :QUERY          => 0x07,
    :RESULT         => 0x08,
    :PREPARE        => 0x09,
    :EXECUTE        => 0x0A,
    :REGISTER       => 0x0B,
    :EVENT          => 0x0C,
    :BATCH          => 0x0D,
    :AUTH_CHALLENGE => 0x0E,
    :AUTH_RESPONSE  => 0x0F,
    :AUTH_SUCCESS   => 0x10,
  }

  @operation_names @operations |> Enum.map(fn {k, v} -> {v, k} end) |> Enum.into(%{})

  @flags %{
    :compression    => 0x01,
    :tracing        => 0x02,
    :custom_payload => 0x04,
    :warning        => 0x08,
  }

  def encode(%__MODULE__{} = f) do
    IO.iodata_to_binary [
      byte(f.version),
      byte(names_to_flag(f.flags, @flags)),
      signed_short(f.stream),
      byte(Map.fetch!(@operations, f.operation)),
      int(byte_size(f.body)),
      f.body,
    ]
  end

  def body_length(<<_::40, length::integer-32>>), do: {:ok, length}
  def body_length(_), do: CQL.Error.new("invalid body length")

  def is_error?(<<_::32, 0::integer-8, _::binary>>), do: true
  def is_error?(_), do: false

  def decode_header(<<
      version::integer-8,
      flags::integer-8,
      stream::signed-integer-16,
      opcode::integer-8,
      length::integer-32
    >>)
  do
    {:ok, %{version: version,
      flags: flags,
      stream: stream,
      opcode: opcode,
      length: length,
    }}
  end
  def decode_header(_), do: CQL.Error.new("invalid header")

  def decode(<<
      version::integer-8,
      flags::integer-8,
      stream::signed-integer-16,
      opcode::integer-8,
      length::integer-32,
      body::binary-size(length),
    >>)
  do
    flags = Decoder.flag_to_names(flags, @flags)

    {tracing_id, body} =
      if :tracing in flags do
        Decoder.uuid(body)
      else
        {nil, body}
      end

    {warnings, body} =
      if :warning in flags do
        Decoder.string_list(body)
      else
        {[], body}
      end

    frame = %__MODULE__{
      version: version,
      flags: flags,
      warnings: warnings,
      tracing_id: tracing_id,
      stream: stream,
      operation: Map.fetch!(@operation_names, opcode),
      length: length,
      body: body,
    }

    {:ok, frame}
  end

  def decode(_), do: CQL.Error.new("invalid frame")

  def set_stream_id(<<head::bits-16, _::signed-integer-16, tail::binary>>, id) do
    {:ok, <<head::bits-16, id::signed-integer-16, tail::binary>>}
  end

  def set_stream_id(_, _), do: CQL.Error.new("invalid frame")
end
