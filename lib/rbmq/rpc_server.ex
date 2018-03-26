defmodule RBMQ.RpcServer do
  @moduledoc """
  RPC Server
  """

  @doc false
  defmacro __using__(opts) do
    quote do
      use RBMQ.Consumer, unquote(opts)

      def handle_delivery(payload, meta, state) do        
        safe_run fn(channel) ->
          AMQP.Basic.ack(channel, meta.delivery_tag)

          Task.start(fn ->
            {type, response} =
              try do
                response = payload
                  |> Jason.decode!
                  |> call(meta)

                {"rpc-call-success", response}
              rescue  
                e ->
                  msg = if Exception.exception?(e),
                    do: Exception.message(e),
                    else: "Unknown error"
                  {"rpc-call-error", %{message: msg, stacktrace: Exception.format_stacktrace()}}
              end
            
            response = response |> Jason.encode!
            :ok = AMQP.Basic.publish(channel, "", meta.reply_to, response, type: type, correlation_id: meta.correlation_id)
          end)
        end
        {:noreply, state}
      end

      def call(payload, _meta), do: call(payload)

      def call(payload) do
        raise "RPC callback is not implemented."
      end

      defoverridable [call: 1, call: 2]
    end
  end
end
