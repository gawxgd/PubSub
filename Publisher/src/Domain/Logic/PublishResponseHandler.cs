using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using Publisher.Outbound.Exceptions;

namespace Publisher.Domain.Logic;

public sealed class PublishResponseHandler
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<PublishResponseHandler>(LogSource.Publisher);

    private long _acknowledgedCount;
    private readonly object _lock = new();
    private TaskCompletionSource<bool>? _waitingForAck;
    private long _targetAckCount;
    
    public long AcknowledgedCount => Interlocked.Read(ref _acknowledgedCount);

    public void Handle(PublishResponse response)
    {
        if (response.ErrorCode != ErrorCode.None)
        {
            Logger.LogError($"Broker returned error: errorCode={response.ErrorCode}, baseOffset={response.BaseOffset}");

            if (response.ErrorCode == ErrorCode.TopicNotFound || response.ErrorCode == ErrorCode.InvalidTopic)
            {
                throw new PublisherException($"Broker error: {response.ErrorCode}");
            }
        }
        else
        {
            Logger.LogDebug($"Received successful response: baseOffset={response.BaseOffset}");
            var newCount = Interlocked.Increment(ref _acknowledgedCount);
            
            lock (_lock)
            {
                if (_waitingForAck != null && newCount >= _targetAckCount)
                {
                    _waitingForAck.TrySetResult(true);
                }
            }
        }
    }

    public async Task<bool> WaitForAcknowledgmentsAsync(long targetCount, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Read(ref _acknowledgedCount) >= targetCount)
        {
            return true;
        }

        TaskCompletionSource<bool> tcs;
        lock (_lock)
        {
            _targetAckCount = targetCount;
            _waitingForAck = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcs = _waitingForAck;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        try
        {
            var delayTask = Task.Delay(Timeout.Infinite, cts.Token);
            var completedTask = await Task.WhenAny(tcs.Task, delayTask);
            
            if (completedTask == tcs.Task)
            {
                return await tcs.Task;
            }
            
            return false; // Timeout
        }
        catch (OperationCanceledException)
        {
            return Interlocked.Read(ref _acknowledgedCount) >= targetCount;
        }
        finally
        {
            lock (_lock)
            {
                _waitingForAck = null;
            }
        }
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _acknowledgedCount, 0);
    }
}

