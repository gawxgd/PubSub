using System.Buffers.Binary;
using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using SchemaRegistryClient;
using Subscriber.Domain;
using Subscriber.Outbound.Exceptions;

namespace Subscriber.Outbound.Adapter;

public sealed class TcpSubscriber<T>(
    string topic,
    TimeSpan pollInterval,
    uint maxRetryAttempts,
    ISubscriberConnection connection,
    ISchemaRegistryClient schemaRegistryClient,
    IDeserializer<T> deserializer,
    Channel<byte[]> inboundChannel,
    Func<T, Task>? messageHandler,
    Func<Exception, Task>? errorHandler = null)
    : ISubscriber<T>, IAsyncDisposable where T : new()
{
    private readonly CancellationTokenSource _cts = new();
    private CancellationToken CancellationToken => _cts.Token;

    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriber<T>>(LogSource.MessageBroker);

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
                Logger.LogDebug($" Retry {retryCount}: {ex.Message}. Waiting {delay}...");
                await Task.Delay(delay);
            }
        }

        throw new SubscriberConnectionException("Max retry attempts exceeded", null);
    }

    public async Task ReceiveAsync(byte[] message)
    {
        if (message.Length < 4)
        {
            Logger.LogError($"Message too short: {message.Length} bytes (minimum 4 for schemaId)");
            return;
        }
        
        var schemaId = BinaryPrimitives.ReadInt32BigEndian(message.AsSpan(0, 4));

        var avroData = message.AsSpan(4).ToArray();

        var writerSchema = await schemaRegistryClient.GetSchemaByIdAsync(schemaId);

        var readerSchema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic);
        
        var deserializedEvent = await deserializer.DeserializeAsync(
            avroData, 
            writerSchema, 
            readerSchema);
        
        try
        {
            if (messageHandler != null)
            {
                await messageHandler(deserializedEvent);
            }
        }
        catch (Exception ex)
        { 
            Logger.LogError( ex.Message);
            
            if (errorHandler != null)
            {
                await errorHandler(ex);
            }
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
            await ((ISubscriber<T>)this).CreateConnection();
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
