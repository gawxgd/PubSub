using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain.UseCase;
using Subscriber.Outbound.Adapter;
using Subscriber.Outbound.Exceptions;

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

        await requestSender.SendRequestAsync(getCurrentOffset());

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var waitTask = responseChannel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                var delayTask = Task.Delay(maxWaitTime, cancellationToken);

                var completedTask = await Task.WhenAny(waitTask, delayTask);

                var highestOffsetProcessed = getCurrentOffset();

                if (completedTask == waitTask && waitTask.Result)
                {
                    while (responseChannel.Reader.TryRead(out var message))
                    {
                        try
                        {
                            var (baseOffset, lastOffset) = processMessageUseCase.GetBatchOffsets(message);
                            if (lastOffset < highestOffsetProcessed)
                            {
                                Logger.LogWarning(
                                    $"Skipping already-processed batch: baseOffset={baseOffset}, lastOffset={lastOffset}, currentOffset={highestOffsetProcessed}");
                                continue;
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError($"Error checking batch offsets for deduplication: {ex.Message}");
                        }
                        
                        var offset = await processMessageUseCase.ExecuteAsync(message);
                        var nextOffset = offset + 1;

                        if (nextOffset > highestOffsetProcessed)
                        {
                            highestOffsetProcessed = nextOffset;
                        }
                    }

                    Logger.LogDebug(
                        $"Processed all available messages, sending request for offset {highestOffsetProcessed}");
                    updateOffset(highestOffsetProcessed);
                    await requestSender.SendRequestAsync(highestOffsetProcessed);
                }
                else
                {
                    //Logger.LogDebug(
                    //    $"No messages received in {maxWaitTime}. Sending fetch request again for offset {getCurrentOffset()}");
                    await requestSender.SendRequestAsync(getCurrentOffset());
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SubscriberConnectionException ex)
            {
                Logger.LogWarning($"Connection lost: {ex.Message}. Stopping receiver so TcpSubscriber can reconnect.");
                throw;
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error while processing message: {ex.Message}", ex);
            }
        }

        Logger.LogInfo("Message receiver stopped");
    }
}
