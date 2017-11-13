defmodule RBMQ.Producer do
  @moduledoc """
  AMQP channel producer.

  You must configure connection (queue and exchange) before calling `publish/1` function.
  """

  @doc false
  defmacro __using__(opts) do
    quote do
      use RBMQ.GenQueue, unquote(opts)

      def validate_config(conf) do
        unless conf[:publish][:routing_key] do
          raise "You need to set a routing key in #{__MODULE__} options."
        end

        case conf[:publish][:routing_key] do
          {:system, _, _} -> :ok
          {:system, _} -> :ok
          str when is_binary(str) -> :ok
          unknown -> raise "Routing key for #{__MODULE__} must be a string or env link, " <>
                           "'#{inspect unknown}' given."
        end

        unless conf[:exchange] do
          raise "You need to configure exchange in #{__MODULE__} options."
        end

        unless conf[:exchange][:name] do
          raise "You need to set exchange name in #{__MODULE__} options."
        end

        case conf[:exchange][:name] do
          {:system, _, _} -> :ok
          {:system, _} -> :ok
          str when is_binary(str) -> :ok
          unknown -> raise "Exchange name key for #{__MODULE__} must be a string or env link, " <>
                           "'#{inspect unknown}' given."
        end

        unless conf[:exchange][:type] do
          raise "You need to set exchange name in #{__MODULE__} options."
        end

        unless conf[:exchange][:type] in [:direct, :fanout, :topic, :headers] do
          raise "Incorrect exchange type in #{__MODULE__} options."
        end

        conf
      end

      @doc """
      Publish new message to a linked channel.
      """
      def publish(data, opts \\ []) do
        GenServer.call(__MODULE__, {:publish, data, opts}, :infinity)
      end

      @doc false
      def handle_call({:publish, data, opts}, _from, state) do
        case Poison.encode(data) do
          {:ok, encoded_data} ->
            safe_publish(state, encoded_data, opts)
          {:error, _} = err ->
            {:reply, err, state}
        end
      end

      defp safe_publish(state, data, opts) do
        safe_run fn(chan) ->
          cast(state, chan, data, opts)
        end
      end

      defp cast(state, chan, data, opts) do
        conf = chan_config()

        publish_conf = conf[:publish]
        is_persistent = Keyword.get(publish_conf, :durable, false)
        default_opts = [mandatory: true, persistent: is_persistent]
        routing_key = opts[:routing_key] || publish_conf[:routing_key] 
        
        opts = Keyword.merge(default_opts, opts)

        case AMQP.Basic.publish(chan,
                                conf[:exchange][:name],
                                routing_key,
                                data,
                                opts
                                ) do
          :ok ->
            {:reply, :ok, struct(state, channel: chan)}
          _ ->
            {:reply, :error, struct(state, channel: chan)}
        end
      end

      defoverridable [validate_config: 1]
    end
  end

  @doc """
  Publish new message to a linked channel.

  If channel is down it will keep trying to send message with 3 second timeout.
  """
  @callback publish :: :ok | :error
end
