defmodule Cassandra.Session.Executor do
  use GenServer
  # @behaviour :poolboy_worker

  import Kernel, except: [send: 2]
  alias Cassandra.{LoadBalancing, Statement}

  ### API ###

  def start_link(schema, balancer, task_supervisor, connection_manager, options) do
    GenServer.start_link(__MODULE__, [schema, balancer, task_supervisor, connection_manager, options], name: __MODULE__)
  end

  def execute(executor, %Cassandra.Statement{} = statement) do
    GenServer.call(executor, {:execute, statement})
  end

  def execute(executor, query, options \\ []) do
    statement = Cassandra.Statement.new(query, options)
    execute(executor, statement)
  end

  ### GenServer Callbacks ###

  @doc false
  def init([schema, balancer, task_supervisor, connection_manager, options]) do
    state = %{
      options: options,
      schema: schema,
      balancer: balancer,
      connection_manager: connection_manager,
      task_supervisor: task_supervisor,
    }

    {:ok, state}
  end

  @doc false
  def handle_call({:execute, statement}, _from, state) do
    result =
      statement
      |> LoadBalancing.plan(state.balancer, state.schema, state.connection_manager)
      |> encode

    {:reply, result, state}
  end

  defp encode(%Statement{request: request} = statement) do
    start = :os.perf_counter(:nanosecond)
    encoded = CQL.encode(request)
    encode_time = :os.perf_counter(:nanosecond) - start
    profile = update_profile(statement.profile, %{encode_time: encode_time, start: start})

    with {:ok, cql} <- encoded do
      execute(%{statement | profile: profile, cql: cql})
    end
  end

  defp execute(%Statement{connections: []}) do
    {:error, :max_tries}
  end

  defp execute(%Statement{cql: cql, connections: [connection | connections]} = statement) when is_binary(cql) do
    start = :os.perf_counter(:nanosecond)
    result = Cassandra.Connection.send(connection, cql, :infinity)
    query_time = :os.perf_counter(:nanosecond) - start
    profile = update_profile(statement.profile, %{query_time: query_time})

    case result do
      {:ok, %CQL.Result.Prepared{} = prepared} ->
        execute = %CQL.Execute{prepared: prepared, params: CQL.QueryParams.new(statement.options)}
        encode(%{statement | profile: profile, request: execute})
      {:ok, _} ->
        {result, result_profile(profile)}
      {:error, {_, _}} -> # CQL Error
        {result, result_profile(profile)}
      {:error, _} -> # Connection error so tring other connections
        execute(%{statement | connections: connections})
    end
  end

  defp update_profile(a, b) do
    Map.merge a, b, fn
      :start, x, _ -> x
      _,      x, y -> x + y
    end
  end

  defp result_profile(%{start: start} = profile) do
    total_time = :os.perf_counter(:nanosecond) - start
    profile = Map.delete(profile, :start)
    used_time =
      profile
      |> Map.values
      |> Enum.reduce(&Kernel.+/2)

    Map.merge(profile, %{total_time: total_time, queue_time: total_time - used_time})
  end
end
