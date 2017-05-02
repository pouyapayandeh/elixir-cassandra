defmodule Cassandra.Session.Executor do
  use GenServer
  @behaviour :poolboy_worker

  require Logger

  alias Cassandra.{LoadBalancing, Statement, Cache}
  alias CQL.Result.Prepared

  ### API ###

  def start_link(cluster, options) do
    with {:ok, balancer} <- Keyword.fetch(options, :balancer),
         {:ok, connection_manager} <- Keyword.fetch(options, :connection_manager),
         {:ok, cache} <- Keyword.fetch(options, :cache)
    do
      options = Keyword.drop(options, [:balancer, :connection_manager])
      GenServer.start_link(__MODULE__, [cluster, balancer, connection_manager, cache, options])
    else
      :error -> {:error, :missing_param}
    end
  end

  def execute(executor, query, options, timeout \\ :infinity) when is_binary(query) and is_list(options) do
    GenServer.call(executor, {:execute, query, options}, timeout)
  end

  def stream(executor, query, func, options, timeout \\ :infinity) when is_binary(query) and is_list(options) do
    options = Keyword.put(options, :streamer, func)
    GenServer.call(executor, {:execute, query, options}, timeout)
  end

  ### poolboy_worker callbacks ###

  def start_link([cluster, options]) do
    start_link(cluster, options)
  end

  ### GenServer Callbacks ###

  @doc false
  def init([cluster, balancer, connection_manager, cache, options]) do
    state = %{
      cache: cache,
      options: options,
      cluster: cluster,
      balancer: balancer,
      connection_manager: connection_manager,
    }

    {:ok, state}
  end

  @doc false
  def handle_call({:execute, query, options}, from, state) do
    run_options = Keyword.put(state.options, :log, options[:log])

    query
    |> Statement.new(options, state.options)
    |> Map.put(:streamer, Keyword.get(options, :streamer))
    |> LoadBalancing.plan(state.balancer, state.cluster, state.connection_manager)
    |> run_async(run_options, from)

    {:noreply, state}
  end

  def handle_cast({:run, statement, options, from}, state) do
    GenServer.reply(from, run(statement, options, state.cache))
    {:noreply, state}
  end

  ### Helpers ###

  defp run_async(statement, options, from) do
    GenServer.cast(self(), {:run, statement, options, from})
  end

  defp prepare_on(ip, connection, statement, options, cache) do
    prepare = fn ->
      DBConnection.prepare(connection, statement, Keyword.put(options, :for_cache, true))
    end

    with %Prepared{} = prepared <- Cache.put_new_lazy(cache, cache_key(statement, ip), prepare) do
      Statement.put_prepared(statement, prepared)
    end
  rescue
    error -> {:error, error}
  end

  defp cache_key(%Statement{query: query, options: options}, ip) do
    :erlang.phash2({query, options, ip})
  end

  defp run_on(connection, %Statement{streamer: streamer} = statement, options) do
    if is_nil(streamer) do
      DBConnection.execute(connection, statement, statement.values, options)
    else
      DBConnection.run(connection, &streamer.(DBConnection.stream(&1, statement, statement.values, options)), options)
    end
  rescue
    error -> {:error, error}
  end

  defp run(%Statement{connections: []}, _options, _cache) do
    Cassandra.ConnectionError.new("execute", "no connection")
  end

  defp run(%Statement{connections: [{ip, connection} | connections]} = statement, options, cache) do
    result =
      with %Statement{} = prepared <- prepare_on(ip, connection, statement, options, cache) do
        run_on(connection, prepared, options)
      end

    case result do
      {:error, %CQL.Error{code: :unprepared}} ->
        Cache.delete(cache, cache_key(statement, ip))
        run(statement, options, cache)

      {:error, %CQL.Error{} = error} ->
        error

      {:error, reason} ->
        Logger.warn("#{__MODULE__} got error: #{inspect reason}")
        run(%Statement{statement | connections: connections}, options, cache)

      {:ok, result} -> result
      result        -> result
    end
  end
end
