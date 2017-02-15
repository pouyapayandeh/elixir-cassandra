defmodule Cassandra.ConnectionError do
  defexception [:action, :reason]

  def new(action, reason) do
    struct(__MODULE__, [action: action, reason: reason])
  end

  def message(%__MODULE__{action: action, reason: reason}) do
    "#{action} #{format(reason)}"
  end

  defp format(reason) when is_atom(reason) do
    case :inet.format_error(reason) do
      'unknown POSIX error' -> inspect(reason)
      reason                -> String.Chars.to_string(reason)
    end
  end
  defp format(reason) when is_binary(reason), do: reason
end
