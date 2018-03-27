defmodule Rbmq.Mixfile do
  use Mix.Project

  @version "0.4.0"

  def project do
    [app: :rbmq19,
     version: @version,
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     description: description(),
     package: package(),
     deps: deps()]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [{:jason, "~> 1.0"},
    {:confex, "~> 1.0"},
    {:amqp, "~> 1.0.2"},
    {:benchfella, "~> 0.3", only: [:dev, :test]},
    {:ex_doc, ">= 0.0.0", only: :dev},
    {:dogma, "~> 0.1", only: :dev},
    {:credo, "~> 0.4", only: [:dev, :test]}]
  end

  defp description do
    """
    Simple API for spawning RabbitMQ Producers and Consumers.
    """
  end

  defp package do
    [
     name: :rbmq19,
     files: ["lib", "mix.exs", "README.md", "LICENSE.md"],
     maintainers: ["Pavel Vesnin", "Andrew Dryga", "Robb Wright", "Manuel Pöter"],
     licenses: ["MIT"],
     links: %{"GitHub" => "https://github.com/Lean5/rbmq"}]
  end
end
