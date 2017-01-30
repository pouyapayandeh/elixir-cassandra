defmodule Cassandra.Cache do
  def new(nil), do: :error
  def new(name) do
    options = [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
    ]
    case :ets.new(name, options) do
      ^name -> {:ok, name}
      _     -> :error
    end
  end

  def put_new_lazy(cache, key, func) do
    with :error <- fetch(cache, key),
         {:ok, value} <- func.()
    do
      put(cache, key, value)
    else
      {:ok, value} -> value
      error        -> error
    end
  end

  def fetch(cache, key) do
    case :ets.lookup(cache, key) do
      [{^key, value}] -> {:ok, value}
      _               -> :error
    end
  end

  def put(cache, key, value) do
    :ets.insert(cache, {key, value})
    value
  end
end
