defmodule Cassandra.UUID do
  @moduledoc false
  use GenServer

  ### API ###

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def v1, do: GenServer.call(__MODULE__, :v1)
  def v4, do: GenServer.call(__MODULE__, :v4)

  ### GenServer Callbacks ###

  def init(_) do
    {:ok, {get_clock_sequense(), get_node()}}
  end

  def handle_call(:v1, _from, {clock_sequense, node} = state) do
    {:reply, UUID.uuid1(clock_sequense, node), state}
  end

  def handle_call(:v4, _from, state) do
    {:reply, UUID.uuid4, state}
  end

  ### Helpers ###

  defp get_node(), do: :inet.getifaddrs |> get_node
  defp get_node({:ok, list}), do: get_node(list)
  defp get_node([{_if_name, if_config} | rest]) do
    case :lists.keyfind(:hwaddr, 1, if_config) do
      :false ->
        get_node(rest)
      {:hwaddr, hw_addr} ->
        if Enum.all?(hw_addr, fn(n) -> n == 0 end) do
          get_node(rest)
        else
          :erlang.list_to_binary(hw_addr)
        end
    end
  end
  defp get_node(_) do
    <<rnd_hi::7, _::1, rnd_low::40>> = :crypto.strong_rand_bytes(6)
    <<rnd_hi::7, 1::1, rnd_low::40>>
  end

  defp get_clock_sequense() do
    <<rnd::14, _::2>> = :crypto.strong_rand_bytes(2)
    <<rnd::14>>
  end
end
