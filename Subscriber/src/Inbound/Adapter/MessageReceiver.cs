using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain.UseCase;
using Subscriber.Outbound.Adapter;
using Subscriber.Outbound.Exceptions;

namespace Subscriber.Inbound.Adapter;

public class MessageReceiver<T>
    where T : new()
{
    private readonly Channel<byte[]> _responseChannel;
    private readonly ProcessMessageUseCase<T> _processMessageUseCase;
    private readonly RequestSender _requestSender; 
    private readonly Func<ulong> _getCurrentOffset;
    private readonly Action<ulong> _updateOffset;
    private readonly TimeSpan _maxWaitTime;
    private readonly CancellationToken _cancellationToken;

    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageReceiver<T>>(LogSource.Subscriber);

    public MessageReceiver(
        Channel<byte[]> responseChannel,
        ProcessMessageUseCase<T> processMessageUseCase,
        RequestSender requestSender,
        Func<ulong> getCurrentOffset,
        Action<ulong> updateOffset,
        TimeSpan maxWaitTime,
        CancellationToken cancellationToken)
    {
        _responseChannel = responseChannel;
        _processMessageUseCase = processMessageUseCase;
        _requestSender = requestSender;
        _getCurrentOffset = getCurrentOffset;
        _updateOffset = updateOffset;
        _maxWaitTime = maxWaitTime;
        _cancellationToken = cancellationToken;
    }

    public async Task StartReceivingAsync()
    {
        Logger.LogInfo("Starting message receiver");

        await _requestSender.SendRequestAsync(_getCurrentOffset());

        while (!_cancellationToken.IsCancellationRequested)
        {
            try
            {
                var waitTask = _responseChannel.Reader.WaitToReadAsync(_cancellationToken).AsTask();
                var delayTask = Task.Delay(_maxWaitTime, _cancellationToken);

                var completedTask = await Task.WhenAny(waitTask, delayTask);

                var highestOffsetProcessed = _getCurrentOffset();

                if (completedTask == waitTask && waitTask.Result)
                {
                    while (_responseChannel.Reader.TryRead(out var message))
                    {
                        ulong offset;
                        try
                        {
                            offset = await _processMessageUseCase.ExecuteAsync(message);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError($"Error processing message: {ex.Message}", ex);
                            throw;
                        }

                        var nextOffset = offset + 1;
                        if (nextOffset > highestOffsetProcessed)
                            highestOffsetProcessed = nextOffset;
                    }

                    _updateOffset(highestOffsetProcessed);

                    await _requestSender.SendRequestAsync(highestOffsetProcessed);
                }
                else
                {
                    await _requestSender.SendRequestAsync(_getCurrentOffset());
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
                Logger.LogError($"Unexpected error: {ex.Message}", ex);
                throw;
            }
        }

        Logger.LogInfo("Message receiver stopped");
    }
}
