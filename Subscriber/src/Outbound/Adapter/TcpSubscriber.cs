<<<<<<< HEAD
using System.Threading.Channels;
=======
﻿using System.Threading.Channels;
>>>>>>> main
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain;
using Subscriber.Inbound.Adapter;
using Subscriber.Outbound.Exceptions;

namespace Subscriber.Outbound.Adapter;

public sealed class TcpSubscriber(
    string topic,
    int minMessageLength,
    int maxMessageLength,
    TimeSpan pollInterval,
    uint maxRetryAttempts,
    ISubscriberConnection connection,
    Channel<byte[]> respondChannel,
    Channel<byte[]> requestChannel,
    Func<string, Task>? messageHandler,
    Func<Exception, Task>? errorHandler = null)
    : ISubscriber, IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private CancellationToken CancellationToken => _cts.Token;

    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriber>(LogSource.MessageBroker);

    private readonly MessageValidator _messageValidator = new(topic, minMessageLength, maxMessageLength);
    private MessageReceiver? _messageReceiver;
    private RequestSender? _requestSender;

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
        // This method is kept for backward compatibility
        // The actual processing is now done by MessageReceiver
        var validationResult = _messageValidator.Validate(message);
        
        if (!validationResult.IsValid)
        {
            return;
        }

        try
        {
            if (messageHandler != null)
            {
                await messageHandler(validationResult.Payload!);
            }
            Logger.LogInfo($"Received message: {validationResult.Payload}");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error handling message: {ex.Message}", ex);
        }
    }
    
    public async Task StartMessageProcessingAsync()
    {
        _messageReceiver = new MessageReceiver(
            respondChannel,
            _messageValidator,
            messageHandler,
            pollInterval,
            CancellationToken);

        await _messageReceiver.StartReceivingAsync();
    }
    
    public async Task StartConnectionAsync()
    {
        try
        {
            await ((ISubscriber)this).CreateConnection();
            
            // Start automatic request sender after connection is established
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
        // 1. Cancel internal operations
        await _cts.CancelAsync();

        // 2. Signal no more outbound messages (requests to broker)
        requestChannel.Writer.TryComplete();
        await requestChannel.Reader.Completion;  // wait until producer finishes

        // 3. Stop TCP read/write loops
        await connection.DisconnectAsync();
        // 4. Signal no more inbound messages (responses from broker)
        respondChannel.Writer.TryComplete();
        await respondChannel.Reader.Completion;   // wait until consumer finishes

        // 5. Cleanup
        _cts.Dispose();
    }
}
