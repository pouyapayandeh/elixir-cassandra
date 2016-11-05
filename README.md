# Cassandra

[![Build Status](https://travis-ci.org/cafebazaar/elixir-cassandra.svg?branch=master)](https://travis-ci.org/cafebazaar/elixir-cassandra)
[![Hex.pm](https://img.shields.io/hexpm/v/cassandra.svg?maxAge=2592000)](https://hex.pm/packages/cassandra)
[![Hex.pm](https://img.shields.io/hexpm/l/cassandra.svg?maxAge=2592000)](https://github.com/cafebazaar/elixir-cassandra/blob/master/LICENSE.md)

An Elixir driver for Apache Cassandra.

This driver works with Cassandra Query Language version 3 (CQL3) and Cassandra's native protocol v4.

## Features

* Automatic peer discovery
* Configurable load-balancing/retry/reconnection policies
* Ecto like Repo supervisor
* Asynchronous execution through Tasks
* Prepared statements with named and position based values

## Todo

* [ ] Batch statement
* [ ] Token based load-balancing policy
* [ ] Compression
* [ ] Authentication and SSL encryption
* [ ] User Defined Types
* [ ] Use prepared `result_metadata` optimization

## Installation

Add `cassandra` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:cassandra, "~> 1.0.0-beta"}]
end
```

## Quick Start

```elixir
defmodule Repo do
  use Cassandra
end

{:ok, _} = Repo.start_link
# uses "127.0.0.1:9042" as contact point by default
# discovers other nodes on first connection

{:ok, _} = Repo.execute """
  CREATE KEYSPACE IF NOT EXISTS test
    WITH replication = {'class':'SimpleStrategy','replication_factor':1};
  """, consistency: :all

{:ok, _} = Repo.execute """
  CREATE TABLE IF NOT EXISTS test.users (
    id timeuuid,
    name varchar,
    age int,
    PRIMARY KEY (id)
  );
  """, consistency: :all

{:ok, insert} = Repo.prepare """
  INSERT INTO test.users (id, name, age) VALUES (now(), ?, ?);
  """

users = [
  %{name: "Bilbo", age: 50},
  %{name: "Frodo", age: 33},
  %{name: "Gandolf", age: 2019},
]

users
|> Enum.map(&Task.async(fn -> Repo.execute(insert, values: &1, consistency: :all) end))
|> Enum.map(&Task.await/1)
|> Enum.each(&IO.inspect(&1))

{:ok, rows} = Repo.execute("SELECT * FROM test.users;", consistency: :all)

# {:ok,
#  %CQL.Result.Rows{
#   columns: ["id", "age", "name"],
#   rows_count: 3,
#   rows: [
#    ["831e5df2-a0e1-11e6-b9af-6d2c86545d91", 2019, "Gandolf"],
#    ["831e5df1-a0e1-11e6-b9af-6d2c86545d91", 33, "Frodo"],
#    ["831e5df0-a0e1-11e6-b9af-6d2c86545d91", 50, "Bilbo"]
#   ]
# }}
```

