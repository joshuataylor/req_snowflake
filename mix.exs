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
      {:req, github: "wojtekmach/req"},
      {:table, "~> 0.1.1", optional: true},
      {:jason, "~> 1.2"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false, optional: true},
      {:elixir_uuid, "~> 1.2"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false, optional: true},
      {:bypass, "~> 2.1", only: :test}
    ]
  end

  def aliases do
    ["test.all": ["test --include integration"]]
  end
end
