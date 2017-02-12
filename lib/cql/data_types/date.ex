defmodule CQL.DataTypes.Date do
  @moduledoc false

  @epoch :calendar.date_to_gregorian_days({1970, 1, 1}) - trunc(:math.pow(2, 31))

  def decode(<<days::integer-32, rest::bytes>>) do
    {:ok, date} =
      days + @epoch
      |> :calendar.gregorian_days_to_date
      |> Date.from_erl

    {date, rest}
  end

  def encode(%Date{} = date), do: date |> Date.to_erl |> encode
  def encode({_, _, _} = date) do
    days = :calendar.date_to_gregorian_days(date)
    n = days - @epoch
    <<n::integer-32>>
  end

  def encode(value) do
    CQL.DataTypes.Encoder.invalid(:date, value)
  end
end
