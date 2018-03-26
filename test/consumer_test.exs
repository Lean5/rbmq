defmodule RBMQ.ConsumerTest do
  use ExUnit.Case, async: false
  use AMQP
  doctest RBMQ.Consumer

  @queue "consumer_test_queue"

  defmodule ProducerTestConnection4Cons do
    use RBMQ.Connection,
      otp_app: :rbmq
  end

  defmodule TestProducer do
    use RBMQ.Producer,
      connection: ProducerTestConnection4Cons,
      publish: [
        routing_key: "consumer_test_queue",
        durable: false
      ],
      exchange: [
        name: "consumer_test_queue_exchange",
        type: :direct,
        durable: false
      ]
  end

  defmodule TestConsumer do
    use RBMQ.Consumer,
      connection: ProducerTestConnection4Cons,
      queue: [
        name: "consumer_test_queue",
        error_name: "consumer_test_queue_error",
        routing_key: ["consumer_test_queue", "another_consumer_test_queue"],
        durable: false
      ],
      exchange: [
        name: "consumer_test_queue_exchange",
        type: :direct,
        durable: false
      ],
      qos: [
        prefetch_count: 10
      ]

    def consume(payload, %{delivery_tag: tag}) do
      :ets.insert_new(:consumer_table, {tag, payload |> Jason.decode!})
      ack(tag)
    end
  end

  setup_all do
    ProducerTestConnection4Cons.start_link
    TestProducer.start_link
    TestConsumer.start_link
    :ok
  end

  setup do
    chan = ProducerTestConnection4Cons.get_channel(RBMQ.ConsumerTest.TestConsumer.Channel)
    AMQP.Queue.purge(chan, @queue)
    :ets.new(:consumer_table, [:named_table, :public])
    [channel: chan]
  end

  test "read messages", context do
    assert {:ok, %{message_count: 0, queue: @queue}} = get_queue_status(context.channel)

    assert :ok == TestProducer.publish(%{example: true})
    assert :ok == TestProducer.publish(1)
    assert :ok == TestProducer.publish("string")
    assert :ok == TestProducer.publish([:list])
    assert :ok == TestProducer.publish(false)

    :timer.sleep(200)

    assert {:ok, %{message_count: 0, queue: @queue}} = get_queue_status(context.channel)
    assert 5 == :ets.match_object(:consumer_table, :"$1") |> Enum.count
  end

   test "reads messages when channel dies", context do
    for n <- 1..100 do
      assert :ok == TestProducer.publish(n)
      if n == 20 do
        # Kill channel
        AMQP.Channel.close(context[:channel])
        :timer.sleep(1) # Break execution loop
      end
    end


    # Wait till it respawns
    :timer.sleep(5_000)

    assert {:ok, %{message_count: 0, queue: @queue}} =
      ProducerTestConnection4Cons.get_channel(RBMQ.ConsumerTest.TestConsumer.Channel)
      |> get_queue_status()    
    assert :ets.match_object(:consumer_table, :"$1") |> Enum.count >= 20
  end

  test "consumes messages from all specified routing keys", context do
    value = "another queue"
    assert :ok == TestProducer.publish(value, routing_key: "another_consumer_test_queue")
    :timer.sleep(100)
    assert {:ok, %{message_count: 0, queue: @queue}} = get_queue_status(context.channel)
    assert [{_, ^value}] = :ets.match_object(:consumer_table, :"$1")
  end

  defp get_queue_status(channel) do
    AMQP.Queue.status(channel, @queue)
  end
end
