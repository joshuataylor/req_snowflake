defmodule ReqSnowflake.MixProject do
  use Mix.Project

  @version "0.1.0-dev"
  @source_url "https://github.com/joshuataylor/req_snowflake"

  def project do
    [
      app: :req_snowflake,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      docs: docs(),
      preferred_cli_env: [
        "test.all": :test,
        docs: :docs,
        "hex.publish": :docs
      ],
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["README.md"]
    ]
  end

  defp deps do
    [
      {:decimal, "~> 2.0"},
      {:req, "~> 0.3.0"},
      {:snowflake_arrow, github: "joshuataylor/snowflake_arrow"},
      {:table, "~> 0.1.2", optional: true},
      {:jason, "~> 1.2", optional: true},
      {:jiffy, "~> 1.1", optional: true},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false, optional: true},
      {:elixir_uuid, "~> 1.2"},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false, optional: true},
      {:bypass, "~> 2.1", only: :test},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false, optional: true},
      {:tz, "~> 0.21.1"},
      {:tz_extra, "~> 0.21.1"},
      {:benchee, "~> 1.1", optional: true},
      {:ezstd, "~> 1.0", optional: true}
    ]
  end

  def aliases do
    ["test.all": ["test --include integration"]]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
