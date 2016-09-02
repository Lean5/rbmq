defmodule RBMQ.GenQueue do
  @moduledoc false

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use GenServer
      require Logger

      @connection Keyword.get(opts, :connection)
      @channel_name String.to_atom("#{__MODULE__}.Channel")

      inline_conf = Keyword.delete(opts, :connection)
      case opts[:otp_app] do
        nil ->
          @channel_conf RBMQ.Config.substitute_env(inline_conf)
        otp_app ->
          @channel_conf inline_conf
          |> Keyword.merge([otp_app: otp_app])
          |> (&RBMQ.Config.get(__MODULE__, &1)).()
      end

      unless @connection do
        raise "You need to implement connection module and pass it in :connection option."
      end

      unless @channel_conf[:queue] do
        raise "You need to configure queue in #{__MODULE__} options."
      end

      unless @channel_conf[:queue][:name] do
        raise "You need to set queue name in #{__MODULE__} options."
      end

      case @channel_conf[:queue][:name] do
        {:system, _, _} -> :ok
        {:system, _} -> :ok
        str when is_binary(str) -> :ok
        unknown -> raise "Queue name for #{__MODULE__} must be a string or env link, '#{inspect unknown}' given."
      end

      def start_link do
        GenServer.start_link(__MODULE__, @channel_conf, name: __MODULE__)
      end

      def init(opts) do
        case Process.whereis(@connection) do
          nil ->
            # Connection doesn't exist, lets fail to recover later
            {:error, :noconn}
          _ ->
            @connection.spawn_channel(@channel_name)
            @connection.configure_channel(@channel_name, opts)

            chan = get_channel
            |> init_worker(opts)

            {:ok, chan}
        end
      end

      defp init_worker(chan, _opts) do
        chan
      end

      defp get_channel do
        chan = @channel_name
        |> @connection.get_channel
      end

      def status do
        GenServer.call(__MODULE__, :status)
      end

      defp config do
        RBMQ.Connection.Channel.get_config(@channel_name)
      end

      def handle_call(:status, _from, chan) do
        safe_run fn(_) ->
          {:reply, AMQP.Queue.status(chan, config[:queue][:name]), chan}
        end
      end

      def safe_run(fun) do
        chan = get_channel

        case !is_nil(chan) && Process.alive?(chan.pid) do
          true ->
            fun.(chan)
          _ ->
            Logger.warn("[GenQueue] Channel #{inspect @channel_name} is dead, waiting till it gets restarted")
            :timer.sleep(3_000)
            safe_run(fun)
        end
      end

      defoverridable [init_worker: 2]
    end
  end

  @doc """
  Create a link to worker process. Used in supervisors.
  """
  @callback start_link :: Supervisor.on_start

  @doc """
  Get queue status.
  """
  @callback status :: {:ok, %{consumer_count: integer,
                              message_count: integer,
                              queue: String.t()}}
                    | {:error, String.t()}
end
