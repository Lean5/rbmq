defmodule RBMQ.RpcTest do
  use ExUnit.Case, async: false
  import RBMQ.Connection
  use AMQP

  defmodule RpcTestConnection do
    use RBMQ.Connection,
      otp_app: :rbmq
  end

  @queue "rpc_test_queue"

  defmodule RpcTestClient do
    use RBMQ.RpcClient,
      connection: RpcTestConnection,
      publish: [
        routing_key: "rpc_test_queue",
        durable: false
      ],
      exchange: [
        name: "rpc_exchange",
        type: :direct,
        durable: false
      ]
  end

  defmodule RpcTestServer do
    use RBMQ.RpcServer,
      connection: RpcTestConnection,
      queue: [
        name: "rpc_test_queue",
        routing_key: "rpc_test_queue",
        durable: false
      ],
      exchange: [
        name: "rpc_exchange",
        type: :direct,
        durable: false
      ]

    def call(payload) do
      if payload == "crash",
        do: raise payload

      %{result: payload |> String.upcase}
    end
  end

  defmodule SlowRpcTestServer do
    use RBMQ.RpcServer,
      connection: RpcTestConnection,
      queue: [
        name: "rpc_test_queue_slow",
        routing_key: "rpc_test_queue_slow",
        durable: false
      ],
      exchange: [
        name: "rpc_exchange",
        type: :direct,
        durable: false
      ]

    def call(payload) do
      Process.sleep(100)
      %{result: payload |> String.upcase}
    end
  end

  setup_all do
    RpcTestConnection.start_link
    RpcTestClient.start_link
    RpcTestServer.start_link
    SlowRpcTestServer.start_link
    :ok
  end

  setup do
    client_chan = RpcTestConnection.get_channel(RBMQ.RpcTest.RpcTestClient.Channel)
    server_chan = RpcTestConnection.get_channel(RBMQ.RpcTest.RpcTestServer.Channel)
    {:ok, _} = AMQP.Queue.declare(server_chan, @queue)
    AMQP.Queue.purge(server_chan, @queue)

    [client: client_chan, server: server_chan]
  end

  test "call returns :ok tuple with result when successful" do
    assert {:ok, %{"result" => "FOO"}} = RpcTestClient.call("foo")
  end

  test "call returns :error tuple with exception info when not successful" do
    assert {:error, %{"message" => "crash"}} = RpcTestClient.call("crash")
  end

  test "call! raises error when not successful" do
    assert_raise RuntimeError, "crash", fn -> RpcTestClient.call!("crash") end
  end

  test "call! returns raw string when :raw is set to true" do
    assert {:ok, "{\"result\":\"FOO\"}"} = RpcTestClient.call("foo", raw: true)
  end

  test "perform many parallel calls, each getting the correct response" do
    for n <- 1..1000 do
      Task.async(fn ->
        value = Integer.to_string(n)
        assert %{ "result" => ^value } = RpcTestClient.call!(value)
      end)
    end
    |> Enum.each(&Task.await/1)
  end

  test "call timeout" do
    assert {:error, {:timeout, _}} = RpcTestClient.call("foo", timeout: 10, routing_key: "rpc_test_queue_slow")
  end
end
