defmodule Cassandra.Cluster.Schema.Partitioner.Murmur3Test do
  use ExUnit.Case, async: true

  alias Cassandra.Cluster.Schema.Partitioner.Murmur3

  test "#create_token" do
    tests = [
      {"123", -7468325962851647638},
      {String.duplicate("\x00\xff\x10\xfa\x99", 10), 5837342703291459765},
      {String.duplicate("\xfe", 8), -8927430733708461935},
      {String.duplicate("\x10", 8), 1446172840243228796},
      {"9223372036854775807", 7162290910810015547},
    ]
    for {parition_key, token} <- tests do
      assert token == Murmur3.create_token(parition_key)
    end
  end

  test "#parse_token" do
    tests = [
      {"-7468325962851647638", -7468325962851647638},
      {"5837342703291459765", 5837342703291459765},
      {"-8927430733708461935", -8927430733708461935},
      {"1446172840243228796", 1446172840243228796},
      {"7162290910810015547", 7162290910810015547},
    ]
    for {string, token} <- tests do
      assert token == Murmur3.parse_token(string)
    end
  end
end
