defmodule Cassandra.Session.Executor do
  use GenServer
  @behaviour :poolboy_worker

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

  def execute(executor, %Statement{} = statement, values) do
    GenServer.call(executor, {:execute, statement, values})
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
  def handle_call({:execute, statement, values}, _from, state) do
    reply =
      statement
      |> Statement.put_values(values)
      |> LoadBalancing.plan(state.balancer, state.cluster, state.connection_manager)
      |> run(state.options, state.cache)

    {:reply, reply, state}
  end

  ### Helpers ###

  defp prepare_on(ip, connection, statement, options, cache) do
    prepare = fn ->
      DBConnection.prepare(connection, statement, Keyword.put(options, :for_cache, true))
    end

    with %Prepared{} = prepared <- Cache.put_new_lazy(cache, cache_key(statement, ip), prepare) do
      Statement.put_prepared(statement, prepared)
    end
  end

  defp execute_on(connection, statement, options) do
    DBConnection.execute(connection, statement, statement.values, options)
  end

  defp cache_key(%Statement{query: query, options: options}, ip) do
    :erlang.phash2({query, options, ip})
  end

  defp run(%Statement{connections: []}, _options, _cache) do
    Cassandra.ConnectionError.new("execute", "no connection")
  end

  defp run(%Statement{connections: [{ip, connection} | connections]} = statement, options, cache) do
    result =
      with %Statement{} = prepared <- prepare_on(ip, connection, statement, options, cache) do
        execute_on(connection, prepared, options)
      end

    case result do
      {:ok, result} ->
        result

      {:error, %CQL.Error{code: :unprepared}} ->
        Cache.delete(cache, cache_key(statement, ip))
        run(statement, options, cache)

      {:error, %CQL.Error{} = error} ->
        error

      {:error, %Cassandra.ConnectionError{}} ->
        run(%Statement{statement | connections: connections}, options, cache)
    end
  end
end
