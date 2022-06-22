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
      ]
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
      {:table, "~> 0.1.1", optional: true},
      {:jason, "~> 1.2", optional: true},
      {:jiffy, "~> 1.1", optional: true},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false, optional: true},
      {:elixir_uuid, "~> 1.2"},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false, optional: true},
      {:bypass, "~> 2.1", only: :test},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false, optional: true},
      {:tz, "~> 0.20.1"},
      {:tz_extra, "~> 0.20.1"},
      {:parallel_stream, "~> 1.1"}
    ]
  end

  def aliases do
    ["test.all": ["test --include integration"]]
  end
end
