using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain.UseCase;
using Subscriber.Outbound.Adapter;

namespace Subscriber.Inbound.Adapter;

public class MessageReceiver<T>(
    Channel<byte[]> responseChannel,
    ProcessMessageUseCase<T> processMessageUseCase,
    RequestSender requestSender,
    Func<ulong> getCurrentOffset,
    Action<ulong> updateOffset,
    TimeSpan maxWaitTime,
    CancellationToken cancellationToken) where T : new()
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageReceiver<T>>(LogSource.Subscriber);

    public async Task StartReceivingAsync()
    {
        Logger.LogInfo("Starting message receiver");

        var currentOffset = getCurrentOffset();
        await requestSender.SendRequestAsync(currentOffset);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var waitTask = responseChannel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                var delayTask = Task.Delay(maxWaitTime, cancellationToken);

                var completedTask = await Task.WhenAny(waitTask, delayTask);

                ulong highestOffsetProcessed = currentOffset;

                if (completedTask == waitTask && waitTask.Result)
                {
                    while (responseChannel.Reader.TryRead(out var message))
                    {
                        var offset = await processMessageUseCase.ExecuteAsync(message);
                        var nextOffset = offset + 1;
                        updateOffset(nextOffset);

                        if (nextOffset > highestOffsetProcessed)
                        {
                            highestOffsetProcessed = nextOffset;
                        }
                    }

                    Logger.LogDebug(
                        $"Processed all available messages, sending request for offset {highestOffsetProcessed}");
                    await requestSender.SendRequestAsync(highestOffsetProcessed);
                }
                else
                {
                    Logger.LogDebug(
                        $"No messages received in {maxWaitTime}. Sending fetch request again for offset {currentOffset}");
                    await requestSender.SendRequestAsync(currentOffset);
                }

                currentOffset = getCurrentOffset();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error while processing message: {ex.Message}", ex);
            }
        }

        Logger.LogInfo("Message receiver stopped");
    }
}