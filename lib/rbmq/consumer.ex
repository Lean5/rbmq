defmodule RBMQ.Consumer do
  @moduledoc """
  AMQP channel consumer.

  TODO: take look at genevent and defimpl Stream (use as Stream) for consumers.
  """

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use RBMQ.GenQueue, opts

      def validate_config(conf) do
        unless conf[:queue] do
          raise "You need to configure queue in #{__MODULE__} options."
        end

        unless conf[:queue][:name] do
          raise "You need to set queue name in #{__MODULE__} options."
        end

        case conf[:queue][:name] do
          {:system, _, _} -> :ok
          {:system, _} -> :ok
          str when is_binary(str) -> :ok
          unknown -> raise "Queue name for #{__MODULE__} must be a string or env link, '#{inspect unknown}' given."
        end

        conf
      end

      def init_worker(chan, opts) do
        link_consumer(chan, opts[:queue][:name])
        chan
      end

      defp link_consumer(chan, queue_name) do
        safe_run fn(chan) ->
          {:ok, _consumer_tag} = AMQP.Basic.consume(chan, queue_name)
          Process.monitor(chan.pid)
        end
      end

      @doc false
      def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
        Process.demonitor(monitor_ref)
        state = link_consumer(nil, chan_config()[:queue][:name])
        {:noreply, state}
      end

      # Confirmation sent by the broker after registering this process as a consumer
      def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
        {:noreply, state}
      end

      # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
      def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
        {:stop, :shutdown, state}
      end

      # Confirmation sent by the broker to the consumer process after a Basic.cancel
      def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
        {:stop, :normal, state}
      end

      # Handle new message delivery
      def handle_info({:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered}}, state) do
        consume(payload, [tag: tag, redelivered?: redelivered])
        {:noreply, state}
      end

      def ack(tag) do
        safe_run fn(chan) ->
          AMQP.Basic.ack(chan, tag)
        end
      end

      def nack(tag) do
        safe_run fn(chan) ->
          AMQP.Basic.nack(chan, tag)
        end
      end

      def cancel(tag) do
        safe_run fn(chan) ->
          AMQP.Basic.cancel(chan, tag)
        end
      end

      def status do
        GenServer.call(__MODULE__, :status)
      end

      def handle_call(:status, _from, chan) do
        safe_run fn(_) ->
          {:reply, AMQP.Queue.status(chan, chan_config()[:queue][:name]), chan}
        end
      end

      def consume(_payload, [tag: tag, redelivered?: _redelivered, channel: chan]) do
        # Mark this message as unprocessed
        nack(tag)
        # Stop consumer from receiving more messages
        cancel(tag)
        raise "#{__MODULE__}.consume/2 is not implemented"
      end

      defoverridable [consume: 2, validate_config: 1]
    end
  end

  @doc """
  Receiver of messages.

  If channel is down it will keep trying to send message with 3 second timeout.
  """
  @callback consume :: :ok | :error
end
