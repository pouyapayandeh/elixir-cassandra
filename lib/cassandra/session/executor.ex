defmodule Cassandra.Session.Executor do
  use GenServer
  @behaviour :poolboy_worker

  import Kernel, except: [send: 2]
  alias Cassandra.{LoadBalancing, Statement}

  ### API ###

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def prepare(executor, %Statement{} = statement) do
    GenServer.call(executor, {:prepare, statement})
  end

  def execute(executor, %Statement{prepared: nil} = statement, values) do
    GenServer.call(executor, {:prepare_and_execute, statement, values})
  end

  def execute(executor, %Statement{} = statement, values) do
    GenServer.call(executor, {:execute, statement, values})
  end

  def execute(executor, query, options) do
    statement = %Statement{query: query, params: options}
    execute(executor, statement, Keyword.get(options,:values, []))
  end

  ### GenServer Callbacks ###

  @doc false
  def init([cluster, options]) do
    with {:ok, balancer} <- Keyword.fetch(options, :balancer),
         {:ok, connection_manager} <- Keyword.fetch(options, :connection_manager)
    do
      state = %{
        options: Keyword.drop(options, [:balancer, :connection_manager]),
        cluster: cluster,
        balancer: balancer,
        connection_manager: connection_manager,
      }

      {:ok, state}
    else
      :error -> {:stop, :missing_param}
    end
  end

  @doc false
  def handle_call({:prepare, statement}, _from, state) do
    reply =
      statement
      |> plan_and_run(:prepare, state)

    {:reply, reply, state}
  end


  @doc false
  def handle_call({:execute, statement, values}, _from, state) do
    reply =
      statement
      |> Statement.put_values(values)
      |> plan_and_run(:execute, state)

    {:reply, reply, state}
  end

  defp plan_and_run(%Statement{} = statement, action, state) do
    statement
    |> LoadBalancing.plan(state.balancer, state.cluster, state.connection_manager)
    |> run(action, state.options)
  end

  defp run(%Statement{connections: []}, _, _options) do
    Cassandra.Connection.Error.new("execute", "no connection")
  end

  defp run(%Statement{connections: [connection | connections]} = statement, :prepare, options) do
    case DBConnection.prepare(connection, statement, options) do
      {:ok, result}                          -> result
      {:error, %CQL.Error{} = error}         -> error
      {:error, %Cassandra.ConnectionError{}} ->
        run(%Statement{statement | connections: connections}, :prepare, options)
    end
  end

  defp run(%Statement{connections: [connection | connections]} = statement, :execute, options) do
    case DBConnection.execute(connection, statement, statement.values, options) do
      {:ok, result}                          -> result
      {:error, %CQL.Error{} = error}         -> error
      {:error, %Cassandra.ConnectionError{}} ->
        run(%Statement{statement | connections: connections}, :execute, options)
    end
  end
end
