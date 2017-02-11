defmodule Cassandra.CacheTest do
  use ExUnit.Case, async: true

  alias Cassandra.Cache

  setup_all do
    {:ok, cache} = Cache.new(__MODULE__)

    {:ok, %{cache: cache}}
  end

  describe "#new" do
    test "without name" do
      assert :error = Cache.new(nil)
    end

    test "with name" do
      assert {:ok, :table_name} = Cache.new(:table_name)
    end
  end

  describe "#fetch" do
    test "fetches given key from cache", %{cache: cache} do
      value = %{this: "is", a: "value"}
      assert ^value = Cache.put(cache, :fetch_test, value)
      assert {:ok, ^value} = Cache.fetch(cache, :fetch_test)
    end

    test "returns :error when key is missing", %{cache: cache} do
      assert :error = Cache.fetch(cache, :missing_key_test)
    end
  end

  describe "#delete" do
    test "deleted given key from cache", %{cache: cache} do
      value = [:value, :to, :delete]
      assert ^value = Cache.put(cache, :delete_test, value)
      assert :ok = Cache.delete(cache, :delete_test)
      assert :error = Cache.fetch(cache, :delete_test)
    end
  end

  describe "#put_new_lazy" do
    test "puts result of function in cache if key is missing", %{cache: cache} do
      value = "the value"
      func = fn -> {:ok, value} end
      assert ^value = Cache.put_new_lazy(cache, :put_new_lazy_test_put, func)
      assert {:ok, ^value} = Cache.fetch(cache, :put_new_lazy_test_put)
    end

    test "with error returning func", %{cache: cache} do
      assert {:error, :some_value} = Cache.put_new_lazy(cache, :put_new_lazy_test_error1, fn -> :some_value end)
      assert {:error, :reason} = Cache.put_new_lazy(cache, :put_new_lazy_test_error2, fn -> {:error, :reason} end)
    end

    test "do not call func if key exists", %{cache: cache} do
      func = fn -> raise "Must not be called" end
      assert 1 = Cache.put(cache, :put_new_lazy_test_call, 1)
      assert 1 = Cache.put_new_lazy(cache, :put_new_lazy_test_call, func)
    end
  end
end
