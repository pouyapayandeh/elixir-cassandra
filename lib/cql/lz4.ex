defmodule CQL.LZ4 do
  def unpack(<<size::unsigned-integer-32, binary::binary>>) do
    :lz4.uncompress(binary, size)
  end
end
