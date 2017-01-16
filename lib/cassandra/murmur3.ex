defmodule Cassandra.Murmur3 do
  @on_load :load_nif

  def x64_128(key, seed \\ 0)

  def x64_128(key, seed) when is_list(key) do
    native_x64_128(key, seed)
  end

  def x64_128(key, seed) when is_binary(key) do
    key
    |> :erlang.binary_to_list
    |> x64_128(seed)
  end

  def x64_128(key, seed) do
    key
    |> :erlang.term_to_binary
    |> x64_128(seed)
  end

  def load_nif do
    path = :filename.join(:code.priv_dir(:cassandra), 'murmur_nif')
    :ok = :erlang.load_nif(path, 0)
  end

  defp native_x64_128(_key, _seed), do: exit(:nif_not_loaded)
end
