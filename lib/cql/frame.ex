defmodule CQL.Frame do
  @moduledoc false

  import CQL.DataTypes.Encoder

  alias CQL.DataTypes.Decoder

  defstruct [
    version: 0x04,
    flags: [],
    stream: 0,
    opration: 0,
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
    Enum.join [
      byte(f.version),
      byte(names_to_flag(f.flags, @flags)),
      short(f.stream),
      byte(Map.fetch!(@operations, f.opration)),
      int(byte_size(f.body)),
      f.body,
    ]
  end

  def decode(<<
      version::integer-8,
      flags::integer-8,
      stream::signed-integer-16,
      opcode::integer-8,
      length::integer-32,
      body::binary-size(length),
      rest::binary,
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
      opration: Map.fetch!(@operation_names, opcode),
      length: length,
      body: body,
    }

    {frame, rest}
  end

  def decode(buffer), do: {nil, buffer}

  def set_stream_id(<<prefix::bits-16, _::signed-integer-16, rest::binary>>, id) do
    {:ok, <<prefix::bits-16, id::signed-integer-16, rest::binary>>}
  end

  def set_stream_id(_, _), do: :error
end
