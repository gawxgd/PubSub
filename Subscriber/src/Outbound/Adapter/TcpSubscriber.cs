using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain;
using Subscriber.Outbound.Exceptions;

namespace Subscriber.Outbound.Adapter;

public sealed class TcpSubscriber(
    string topic,
    int minMessageLength,
    int maxMessageLength,
    TimeSpan pollInterval,
    uint maxRetryAttempts,
    ISubscriberConnection connection,
    Channel<byte[]> inboundChannel,
    Func<string, Task>? messageHandler,
    Func<Exception, Task>? errorHandler = null)
    : ISubscriber, IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private CancellationToken CancellationToken => _cts.Token;

    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriber>(LogSource.MessageBroker);

    async Task ISubscriber.CreateConnection()
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
                Logger.LogDebug($" Retry {retryCount}: {ex.Message}. Waiting {delay}...");
                await Task.Delay(delay);
            }
        }

        throw new SubscriberConnectionException("Max retry attempts exceeded", null);
    }

    public async Task ReceiveAsync(byte[] message)
    {
        //TODO: change topic checking and reading message to avro
        
        var text = Encoding.UTF8.GetString(message);

        if (text.Length < minMessageLength || text.Length > maxMessageLength)
        {
            Logger.LogError($"Invalid message length: {text.Length}");
            return;
        }

        if (!text.StartsWith($"{topic}:"))
        {
            Logger.LogError( $"Ignored message (wrong topic): {text}");
            return;
        }

        var payload = text.Substring(topic.Length + 1);

        try
        {
            if (messageHandler != null)
            {
                await messageHandler(payload);    
            }
            Logger.LogInfo( $"Received message: {payload}");
        }
        catch (Exception ex)
        { 
            Logger.LogError( ex.Message);
        }
    }
    
    public async Task StartMessageProcessingAsync()
    {
        while (!CancellationToken.IsCancellationRequested)
        {
            try
            {
                if (await inboundChannel.Reader.WaitToReadAsync(CancellationToken))
                {
                    while (inboundChannel.Reader.TryRead(out var message))
                    {
                        await ReceiveAsync(message);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error while processing message: {ex.Message}");
            }

            try
            {
                await Task.Delay(pollInterval, CancellationToken);
            }
            catch (TaskCanceledException)
            {
                Logger.LogWarning("Message processing task cancelled");
            }
        }
    }
    
    public async Task StartConnectionAsync()
    {
        try
        {
            await ((ISubscriber)this).CreateConnection();
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
        await _cts.CancelAsync();      // 1. Cancel internal operations
        await connection.DisconnectAsync();                // 2. Stop TCP read loop / producer
        inboundChannel.Writer.TryComplete();               // 3. Signal no more messages
        await inboundChannel.Reader.Completion;            // 4. Wait for consumer to finish
        _cts.Dispose();                // 5. Cleanup

    }
}
