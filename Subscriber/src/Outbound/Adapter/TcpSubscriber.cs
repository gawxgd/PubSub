using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain;
using Subscriber.Domain.UseCase;
using Subscriber.Inbound.Adapter;
using Subscriber.Outbound.Exceptions;

namespace Subscriber.Outbound.Adapter;

public sealed class TcpSubscriber<T>(
    string topic,
    TimeSpan pollInterval,
    uint maxRetryAttempts,
    ISubscriberConnection connection,
    Channel<byte[]> responseChannel,
    Channel<byte[]> requestChannel,
    ProcessMessageUseCase<T> processMessageUseCase)
    : ISubscriber<T>, IAsyncDisposable where T : new()
{
    private readonly CancellationTokenSource _cts = new();
    private CancellationToken CancellationToken => _cts.Token;

    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<TcpSubscriber<T>>(LogSource.MessageBroker);

    private MessageReceiver<T>? _messageReceiver;
    private RequestSender? _requestSender;
    private ulong _lastOffset = 0;

    async Task ISubscriber<T>.CreateConnection()
    {
        var retryCount = 0;

        while (retryCount < maxRetryAttempts && !CancellationToken.IsCancellationRequested)
        {
            try
            {
                await connection.ConnectAsync();
                return;
            }
            catch (SubscriberConnectionException ex)
            {
                retryCount++;
                var delay = TimeSpan.FromSeconds(Math.Min(retryCount, maxRetryAttempts));
                Logger.LogDebug($"Retry {retryCount}: {ex.Message}. Waiting {delay}...");
                await Task.Delay(delay, CancellationToken);
            }
        }

        throw new SubscriberConnectionException("Max retry attempts exceeded", null);
    }

    public async Task StartConnectionAsync(ulong? initialOffset = null)
    {
        try
        {
            await ((ISubscriber<T>)this).CreateConnection();

            _requestSender = new RequestSender(
                requestChannel,
                topic,
                CancellationToken);

            if (initialOffset.HasValue)
            {
                SetInitialOffset(initialOffset.Value);
            }
        }
        catch (SubscriberConnectionException ex) when (ex.IsRetriable)
        {
            Logger.LogWarning($"Retriable connection error: {ex.Message}");
            await Task.Delay(pollInterval, CancellationToken);
            await StartConnectionAsync(initialOffset);
        }
        catch (SubscriberConnectionException ex)
        {
            Logger.LogError($"Unretriable connection error: {ex.Message}");
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogError($"Unexpected error: {ex.Message}");
            throw;
        }
    }

    public async Task StartMessageProcessingAsync()
    {
        if (_requestSender == null)
        {
            Logger.LogWarning("RequestSender is null when starting message processing!");
        }

        while (!CancellationToken.IsCancellationRequested)
        {
            try
            {
                _messageReceiver = new MessageReceiver<T>(
                    responseChannel,
                    processMessageUseCase,
                    _requestSender!,             
                    GetCurrentOffset,
                    UpdateOffset,
                    pollInterval,
                    CancellationToken);

                await _messageReceiver.StartReceivingAsync();
            }
            catch (Exception ex)
            {
                Logger.LogError($"Receiver error: {ex.Message}");
            }

            if (CancellationToken.IsCancellationRequested)
                break;

            await ReconnectAsync();
        }
    }

    private async Task ReconnectAsync()
    {
        Logger.LogInfo("Attempting to reconnect to broker...");

        try
        {
            await connection.DisconnectAsync();
        }
        catch (Exception ex)
        {
            Logger.LogDebug($"Error during disconnect: {ex.Message}");
        }

        var retryCount = 0;

        while (retryCount < maxRetryAttempts && !CancellationToken.IsCancellationRequested)
        {
            try
            {
                await connection.ConnectAsync();
                Logger.LogInfo("Successfully reconnected to broker");

                await _requestSender!.SendRequestAsync(GetCurrentOffset());
                return;
            }
            catch (SubscriberConnectionException ex)
            {
                retryCount++;

                if (retryCount >= maxRetryAttempts)
                {
                    Logger.LogError($"Max reconnect attempts ({maxRetryAttempts}) exceeded");
                    throw;
                }

                var delay = TimeSpan.FromSeconds(Math.Min(retryCount, maxRetryAttempts));
                Logger.LogDebug($"Reconnect attempt {retryCount}/{maxRetryAttempts} failed: {ex.Message}. Waiting {delay}...");
                await Task.Delay(delay, CancellationToken);
            }
        }
    }

    private void SetInitialOffset(ulong offset)
    {
        _lastOffset = offset;
        Logger.LogDebug($"Set initial offset to: {offset}");
    }

    private void UpdateOffset(ulong offset)
    {
        _lastOffset = offset;
        Logger.LogDebug($"Updated last offset to: {offset}");
    }

    private ulong GetCurrentOffset() => _lastOffset;

    public ulong? GetCommittedOffset() =>
        _lastOffset > 0 ? _lastOffset - 1 : 0;

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        requestChannel.Writer.TryComplete();
        responseChannel.Writer.TryComplete();

        if (connection is TcpSubscriberConnection tcpConnection)
        {
            await tcpConnection.DisconnectAndCloseChannelsAsync();
        }
        else
        {
            await connection.DisconnectAsync();
        }

        _cts.Dispose();
    }
}
