defmodule RBMQ.RpcClient do
  @moduledoc """
  RPC Client
  """
  defstruct [channel: nil, continuation_map: %{}, correlation_id: 1]

  @doc false
  defmacro __using__(opts) do
    opts = Keyword.merge(opts, state: __MODULE__)
    
    quote do
      use RBMQ.Producer, unquote(opts)

      @reply_to_queue "amq.rabbitmq.reply-to"
      @call_timeout unquote(Keyword.get(opts, :call_timeout, 5000))

      def call(payload, opts \\ []) do
        timeout = Keyword.get(opts, :timeout, @call_timeout)
        try do
          GenServer.call(__MODULE__, {:call, payload}, timeout)
        catch
          :exit, reason -> {:error, reason}
        end
      end

      def call!(payload, opts \\ []) do        
        case call(payload, opts) do
          {:ok, res} -> res
          {:error, %{"message" => msg} = err} ->
            stacktrace = Map.get(err, "stacktrace", "<NA>")
            Logger.error("[RpcClient] server returned error \"#{msg}\" - stacktrace: #{stacktrace}")
            raise msg

          {:error, reason} ->
            Logger.error("[RpcClient] failed with error #{inspect reason}")
            raise RuntimeError
        end
      end

      def init_worker(state, _opts) do
        link_consumer(state)
        state
      end

      defp link_consumer(state) do
        safe_run fn(chan) ->
          {:ok, _consumer_tag} = AMQP.Basic.consume(chan, @reply_to_queue, nil, no_ack: true)
          Process.monitor(chan.pid)
          struct(state, channel: chan)
        end
      end

      def handle_call({:call, payload}, from, state) do
        case Poison.encode(payload) do
          {:ok, encoded_data} ->
            %{correlation_id: correlation_id,
              continuation_map: continuation_map}  = state

            encoded_correlation_id = correlation_id |> Integer.to_string
            continuation_map = Map.put(continuation_map, encoded_correlation_id, from)
            state = struct(state, [correlation_id: correlation_id + 1, continuation_map: continuation_map])
            safe_call(state, encoded_data, encoded_correlation_id)

          {:error, _} = err ->
            {:reply, err, state}
        end
      end

      defp safe_call(state, data, correlation_id) do
        case safe_publish(state, data, type: "rpc-call", correlation_id: correlation_id, reply_to: @reply_to_queue) do
          {:reply, :error, state} ->
            continuation_map = Map.delete(state.continuation_map, correlation_id)
            struct(state, continuation_map: continuation_map)
            {:reply, :error, state}

          # do not send reply in case of success
          # -> we will send the reply once the response message arrives
          {:reply, :ok, state} ->
            {:noreply, state}
        end
      end

      def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
        Process.demonitor(monitor_ref)
        state = link_consumer(state)
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

      def handle_info({:basic_deliver, payload, meta}, state) do
        %{continuation_map: continuations} = state
        %{correlation_id: correlation_id} = meta
        case Map.get(continuations, correlation_id) do
          nil -> {:noreply, state}
          from ->
            response = payload
              |> Poison.decode!
              |> make_response(meta)
            GenServer.reply(from, response)
            {:noreply, struct(state, continuation_map: Map.delete(continuations, correlation_id))}
        end
      end

      defp make_response(payload, %{type: "rpc-call-success"}), do: {:ok, payload}
      defp make_response(payload, %{type: "rpc-call-error"}), do: {:error, payload}
      defp make_response(_, %{type: type}), do: raise "Unexpected RPC response with message type #{type}."
    end
  end
end
