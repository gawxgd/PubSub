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

    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriber<T>>(LogSource.MessageBroker);

    private MessageReceiver<T>? _messageReceiver;
    private RequestSender? _requestSender;

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
                await Task.Delay(delay);
            }
        }

        throw new SubscriberConnectionException("Max retry attempts exceeded", null);
    }

    public async Task ReceiveAsync(byte[] message)
    {
        await processMessageUseCase.ExecuteAsync(message);
    }

    public async Task StartMessageProcessingAsync()
    {
        _messageReceiver = new MessageReceiver<T>(
            responseChannel,
            processMessageUseCase,
            pollInterval,
            CancellationToken);

        await _messageReceiver.StartReceivingAsync();
    }

    public async Task StartConnectionAsync()
    {
        try
        {
            await ((ISubscriber<T>)this).CreateConnection();

            _requestSender = new RequestSender(
                requestChannel,
                topic,
                pollInterval,
                CancellationToken);

            _ = Task.Run(() => _requestSender.StartSendingAsync(), CancellationToken);
        }
        catch (SubscriberConnectionException ex) when (ex.IsRetriable)
        {
            Logger.LogWarning($"Retriable connection error: {ex.Message}");
            await Task.Delay(pollInterval, CancellationToken);
            await StartConnectionAsync();
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

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        requestChannel.Writer.TryComplete();
        await requestChannel.Reader.Completion;

        await connection.DisconnectAsync();

        responseChannel.Writer.TryComplete();
        await responseChannel.Reader.Completion;

        _cts.Dispose();
    }
}
