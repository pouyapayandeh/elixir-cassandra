defmodule Cassandra.Mixfile do
  use Mix.Project

  def project, do: [
    app: :cassandra,
    version: version(),
    name: "Cassandra",
    elixir: "~> 1.4",
    build_embedded: Mix.env == :prod,
    start_permanent: Mix.env == :prod,
    compilers: [:elixir_make | Mix.compilers],
    test_coverage: [tool: ExCoveralls],
    preferred_cli_env: [
      "coveralls": :test,
      "coveralls.detail": :test,
      "coveralls.post": :test,
      "coveralls.html": :test,
    ],
    source_url: "https://github.com/cafebazaar/elixir-cassandra",
    description: "A pure Elixir driver for Apache Cassandra",
    package: package(),
    docs: docs(),
    deps: deps(),
  ]

  def application, do: [
    mod: {Cassandra, []},
    applications: [:logger, :db_connection],
  ]

  defp deps, do: [
    {:db_connection, "~> 1.1"},
    {:elixir_make, "~> 0.4", runtime: false},
    {:ex_doc, "~> 0.15", only: :dev},
    {:excheck, "~> 0.5", only: :test},
    {:excoveralls, "~> 0.6", only: :test},
    {:lz4, "~> 0.2"},
    {:poolboy, "~> 1.5"},
    {:triq, github: "triqng/triq", only: :test},
    {:uuid, "~> 1.1"},
  ]

  defp version, do: "1.0.0-rc.2"

  defp docs, do: [
    main: "readme",
    extras: ["README.md"],
  ]

  defp package, do: [
    licenses: ["Apache 2.0"],
    maintainers: ["Ali Rajabi", "Hassan Zamani"],
    links: %{
      "Github" => "https://github.com/cafebazaar/elixir-cassandra",
      "Docs" => "https://hexdocs.pm/cassandra/#{version()}/",
    },
    files: ~w(mix.exs lib native Makefile README.md LICENSE.md),
  ]
end
