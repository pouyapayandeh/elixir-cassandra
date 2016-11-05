defprotocol Cassandra.Reconnection.Policy do
  @moduledoc """
  Protocol to specify a reconnection policy
  """

  @doc """
  Returns number of milliseconds to backoff before retrying to connect
  or `:stop` to stop retrying.
  """
  def get(state)

  @doc """
  Returns next policy state
  """
  def next(state)

  @doc """
  Resets the policy state to it's initial state
  """
  def reset(state)
end
