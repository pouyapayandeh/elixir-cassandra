defmodule Cassandra.Murmur3Test do
  use ExUnit.Case, async: true

  test "#x64_128" do
    tests = [
      {"123", -7468325962851647638},
      {String.duplicate("\x00\xff\x10\xfa\x99", 10), 5837342703291459765},
      {String.duplicate("\xfe", 8), -8927430733708461935},
      {String.duplicate("\x10", 8), 1446172840243228796},
      {"9223372036854775807", 7162290910810015547},
      {"\x01\x02\x03\x04\x05\x06\a\b\t\n\v\f\r\x0E\x0F\x10", -5563837382979743776},
      {"\x02\x03\x04\x05\x06\a\b\t\n\v\f\r\x0E\x0F\x10\x11", -1513403162740402161},
      {"\x03\x04\x05\x06\a\b\t\n\v\f\r\x0E\x0F\x10\x11\x12", -495360443712684655},
      {"\x04\x05\x06\a\b\t\n\v\f\r\x0E\x0F\x10\x11\x12\x13", 1734091135765407943},
      {"\x05\x06\a\b\t\n\v\f\r\x0E\x0F\x10\x11\x12\x13\x14", -3199412112042527988},
      {"\x06\a\b\t\n\v\f\r\x0E\x0F\x10\x11\x12\x13\x14\x15", -6316563938475080831},
      {"\a\b\t\n\v\f\r\x0E\x0F\x10\x11\x12\x13\x14\x15\x16", 8228893370679682632},
      {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 5457549051747178710},
      {"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", -2824192546314762522},
      {"\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE", -833317529301936754},
      {"\x00\x01\x02\x03\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 6463632673159404390},
      {"\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE\xFE", -1672437813826982685},
      {"\xFE\xFE\xFE\xFE", 4566408979886474012},
      {"\x00\x00\x00\x00", -3485513579396041028},
      {"\x00\x01\x7F\x7F", 6573459401642635627},
      {"\x00\xFF\xFF\xFF", 123573637386978882},
      {"\xFF\x01\x02\x03", -2839127690952877842},
      {"\x00\x01\x02\x03\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 6463632673159404390},
      {"\xE2\xE7", -8582699461035929883},
      {"\xE2\xE7\xE2\xE7\xE2\xE7\x01", 2222373981930033306},
    ]
    for {string, hash} <- tests do
      assert hash == Cassandra.Murmur3.x64_128(string)
    end
  end
end
