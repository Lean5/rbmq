defmodule RBMQ.ProducerTest do
  use ExUnit.Case, async: false
  import RBMQ.Connection
  use AMQP
  doctest RBMQ.Producer

  defmodule ProducerTestConnection do
    use RBMQ.Connection,
      otp_app: :rbmq
  end

  @queue "producer_test_qeueue"
  @queue2 "producer_test_qeueue_2"

  defmodule TestProducer do
    use RBMQ.Producer,
      connection: ProducerTestConnection,
      publish: [
        routing_key: "producer_test_qeueue",
        durable: false
      ],
      exchange: [
        name: "producer_test_qeueue_exchange",
        type: :direct,
        durable: false
      ]
  end

  defmodule TestProducerWithExternalConfig do
    use RBMQ.Producer,
      otp_app: :rbmq,
      connection: ProducerTestConnection,
      publish: [
        durable: false
      ],
      exchange: [
        name: "producer_test_qeueue_exchange",
        type: :direct,
        durable: false
      ]
  end

  setup_all do
    ProducerTestConnection.start_link
    TestProducer.start_link
    :ok
  end

  setup do
    chan = ProducerTestConnection.get_channel(RBMQ.ProducerTest.TestProducer.Channel)
    {:ok, _} = AMQP.Queue.declare(chan, @queue)
    :ok = AMQP.Queue.bind(chan, @queue, "producer_test_qeueue_exchange", routing_key: "producer_test_qeueue")
    :ok = AMQP.Queue.bind(chan, @queue, "producer_test_qeueue_exchange", routing_key: "ext_producer_test_routing_key")
    AMQP.Queue.purge(chan, @queue)

    {:ok, _} = AMQP.Queue.declare(chan, @queue2)
    :ok = AMQP.Queue.bind(chan, @queue2, "producer_test_qeueue_exchange", routing_key: "custom_routing_key")
    AMQP.Queue.purge(chan, @queue2)

    [channel: chan]
  end

  test "publish message" do
    assert :ok == TestProducer.publish(%{example: true})
    assert :ok == TestProducer.publish(1)
    assert :ok == TestProducer.publish("string")
    assert :ok == TestProducer.publish([:list])
    assert :ok == TestProducer.publish(false)
  end

  test "publish message with custom routing key", context do
    assert :ok == TestProducer.publish("foo", routing_key: "custom_routing_key")
    assert {:ok, %{message_count: 1, queue: @queue2}} = get_queue_status(context[:channel], @queue2)
  end

  test "rapidly publish messages", context do
    TestProducer.publish(%{example: true})

    for n <- 1..1000 do
      assert :ok == TestProducer.publish(n)
    end

    # Doesn't spawn additional connections
    assert Supervisor.count_children(ProducerTestConnection).active == 1
    assert Supervisor.count_children(ProducerTestConnection).workers == 1

    :timer.sleep(500)

    assert {:ok, %{message_count: 1001, queue: @queue}} = get_queue_status(context[:channel])
  end

  test "messages delivered when channel dies", context do
    assert Supervisor.count_children(ProducerTestConnection).active == 1
    assert Supervisor.count_children(ProducerTestConnection).workers == 1

    for n <- 1..100 do
      assert :ok == TestProducer.publish(n)
      if n == 20 do
        # Kill channel
        AMQP.Channel.close(context[:channel])
        :timer.sleep(1) # Break execution loop
      end
    end

    # Wait till it respawns
    :timer.sleep(1_500)

    # Doesn't spawn additional connections
    assert Supervisor.count_children(ProducerTestConnection).active == 1
    assert Supervisor.count_children(ProducerTestConnection).workers == 1

    assert {:ok, %{message_count: 100, queue: @queue}} =
      ProducerTestConnection.get_channel(RBMQ.ProducerTest.TestProducer.Channel)
      |> get_queue_status()
  end

  test "reads external config" do
    System.put_env("CUST_ROUTING_KEY", "ext_producer_test_routing_key")
    TestProducerWithExternalConfig.start_link
    System.delete_env("CUST_ROUTING_KEY")

    assert :ok == TestProducer.publish(%{example: true})
  end

  defp get_queue_status(channel, queue \\ @queue) do
    AMQP.Queue.status(channel, queue)
  end
end
