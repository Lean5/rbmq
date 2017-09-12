defmodule RBMQ.GenQueue do
  @moduledoc false

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use GenServer
      use Confex, Keyword.delete(opts, :connection)
      require Logger

      @connection Keyword.get(opts, :connection) || @module_config[:connection]
      @channel_name String.to_atom("#{__MODULE__}.Channel")

      unless @connection do
        raise "You need to implement connection module and pass it in :connection option."
      end

      def start_link do
        GenServer.start_link(__MODULE__, config(), name: __MODULE__)
      end

      def init(opts) do
        case Process.whereis(@connection) do
          nil ->
            # Connection doesn't exist, lets fail to recover later
            {:error, :noconn}
          _ ->
            @connection.spawn_channel(@channel_name)
            @connection.configure_channel(@channel_name, opts)

            chan = get_channel()
            |> init_worker(opts)

            {:ok, chan}
        end
      end

      def init_worker(chan, _opts) do
        chan
      end

      defp get_channel do
        chan = @channel_name
        |> @connection.get_channel
      end

      def chan_config do
        RBMQ.Connection.Channel.get_config(@channel_name)
      end

      def safe_run(fun) do
        chan = get_channel()

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
