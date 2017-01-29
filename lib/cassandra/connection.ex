defmodule Cassandra.Connection do
  @moduledoc false

  use DBConnection
  require Logger

  alias Cassandra.{Host, Statement, ConnectionError}
  alias CQL.Frame

  @defaults [
    port: 9042,
    host: {127, 0, 0, 1},
    connect_timeout: 1000,
    timeout: 5000,
  ]

  @header_length 9
  @startup_request CQL.encode!(%CQL.Startup{})
  @options_request CQL.encode!(%CQL.Options{})

  ### API ###

  def query(socket, request, timeout \\ @defaults[:timeout]) do
    with :ok <- tcp_send(socket, request),
         {:ok, result} <- receive_response(socket, timeout)
    do
      result
    end
  end

  def stream(options) do
    options
    |> Keyword.get(:contact_points, ["127.0.0.1"])
    |> stream(options)
  end

  def stream(hosts, options) do
    hosts
    |> Stream.map(&{&1, connect(&1, options)})
    |> Stream.filter_map(&ok?/1, fn {host, {:ok, %{socket: socket, options: options}}} -> {host, socket, options} end)
  end

  def connect({host, port}, options) do
    options
    |> Keyword.merge([host: host, port: port])
    |> connect
  end

  def connect(%Host{ip: ip}, options) do
    connect(ip, options)
  end

  def connect(host, options) do
    options
    |> Keyword.put(:host, host)
    |> connect
  end

  ### DBConnection Callbacks ###

  def connect(options) do
    options         = Keyword.merge(@defaults, options)
    host            = get_host(options)
    port            = options[:port]
    timeout         = options[:timeout]
    connect_timeout = options[:connect_timeout]
    tcp_options     = [
      :binary,
      {:active, false},
      {:keepalive, true},
      {:packet, :raw},
    ]
    with {:ok, socket}  <- :gen_tcp.connect(host, port, tcp_options, connect_timeout),
         :ok            <- handshake(socket, timeout),
         {:ok, options} <- fetch_options(socket, timeout)
    do
      {:ok, %{socket: socket, host: host, timeout: timeout, options: options}}
    else
      {:error, reason} when is_atom(reason) -> {:error, ConnectionError.new("TCP Connect", reason)}
      error                                 -> {:error, error}
    end
  end

  def checkin(state),  do: {:ok, state}
  def checkout(state), do: {:ok, state}

  def disconnect(_error, %{socket: socket}) do
    :gen_tcp.close(socket)
  end

  def handle_begin(_options, state) do
    {:error, ConnectionError.new("transaction", "not supported"), state}
  end

  def handle_close(statement, _options, state) do
    {:ok, statement, state}
  end

  def handle_commit(_options, state) do
    {:error, ConnectionError.new("transaction", "not supported"), state}
  end

  # def handle_deallocate(query, cursor, opts, state)
  # def handle_declare(query, params, opts, state) do

  def handle_execute(_statement, %CQL.Error{} = error, _options, state) do
    {:error, error, state}
  end

  def handle_execute(%Statement{}, request, _options, state) do
    with {:ok, result} <- fetch(request, state.socket, state.timeout) do
      {:ok, result, state}
    else
      error -> {:error, error, state}
    end
  end

  # def handle_first(query, cursor, opts, state)
  # def handle_info(msg, state)
  # def handle_next(query, cursor, opts, state)

  def handle_prepare(%CQL.Error{} = error, _options, state) do
    {:error, error, state}
  end

  def handle_prepare(%Statement{} = statement, _options, state) do
    with {:ok, frame} <- fetch(statement.request, state.socket, state.timeout) do
      {:ok, %{statement | response: frame}, state}
    else
      error -> {:error, error, state}
    end
  end

  def handle_rollback(_options, state) do
    {:error, ConnectionError.new("transaction", "not supported"), state}
  end

  def ping(%{socket: socket, timeout: timeout} = state) do
    with {:ok, _options} <- fetch_options(socket, timeout) do
      {:ok, state}
    else
      error -> {:disconnect, error, state}
    end
  end

  ### Helpers ###

  defp fetch(request, socket, timeout) do
    with :ok          <- tcp_send(socket, request),
         {:ok, frame} <- receive_frame(socket, timeout)
    do
      {:ok, frame}
    end
  end

  defp encode_send_receive(request, socket, timeout) do
    with {:ok, request} <- CQL.encode(request) do
      fetch(request, socket, timeout)
    end
  end

  defp tcp_send(socket, request) do
    with :ok <- :gen_tcp.send(socket, request) do
      :ok
    else
      {:error, reason} -> {:error, ConnectionError.new("TCP send", reason)}
    end
  end

  defp tcp_receive(socket, bytes, timeout) do
    with {:ok, data} <- :gen_tcp.recv(socket, bytes, timeout) do
      {:ok, data}
    else
      {:error, reason} -> {:error, ConnectionError.new("TCP receive", reason)}
    end
  end

  defp receive_response(socket, timeout) do
    with {:ok, frame} <- receive_frame(socket, timeout),
         {:ok, %CQL.Frame{body: body, warnings: warnings}} <- CQL.decode(frame)
    do
      Enum.each(warnings, &Logger.warn/1)
      {:ok, body}
    end
  end

  defp receive_frame(socket, timeout) do
    with {:ok, header} <- tcp_receive(socket, @header_length, timeout),
         {:ok, body}   <- receive_body(socket, header, timeout),
         {:ok, frame}  <- CQL.decode_error(header <> body)
    do
      {:ok, frame}
    else
      error = %CQL.Error{} -> error
      {:error, reason}     -> ConnectionError.new("TCP receive", reason)
    end
  end

  defp receive_body(socket, header, timeout) do
    case Frame.body_length(header) do
      {:ok, 0} -> {:ok, <<>>}
      {:ok, n} -> tcp_receive(socket, n, timeout)
      error    -> error
    end
  end

  defp fetch_options(socket, timeout) do
    with :ok <- tcp_send(socket, @options_request),
         {:ok, %CQL.Supported{options: options}} <- receive_response(socket, timeout)
    do
      {:ok, Enum.into(options, %{})}
    end
  end

  defp handshake(socket, timeout) do
    with :ok <- tcp_send(socket, @startup_request),
         {:ok, %CQL.Ready{}} <- receive_response(socket, timeout)
    do
      :ok
    end
  end

  defp get_host(options) do
    case Keyword.get(options, :host) do
      nil                                -> @defaults[:host]
      %Cassandra.Host{ip: ip}            -> ip
      address when is_bitstring(address) -> to_charlist(address)
      inet                               -> inet
    end
  end

  defp ok?({_, {:ok, _}}), do: true
  defp ok?(_),             do: false
end
