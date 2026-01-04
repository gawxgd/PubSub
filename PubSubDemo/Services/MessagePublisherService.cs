using Publisher.Domain.Port;
using PubSubDemo.Configuration;

namespace PubSubDemo.Services;

/// <summary>
/// Background service that publishes demo messages to the broker.
/// </summary>
public sealed class MessagePublisherService<T> : IAsyncDisposable
{
    private readonly IPublisher<T> _publisher;
    private readonly DemoOptions _options;
    private readonly CancellationTokenSource _cts;
    private Task? _publishTask;
    private long _messagesSent;
    private long _messagesFailed;

    public MessagePublisherService(IPublisher<T> publisher, DemoOptions options)
    {
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _cts = new CancellationTokenSource();
    }

    /// <summary>
    /// Starts the publishing service.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Starting Message Publisher Service...");

        try
        {
            // Connect to the broker
            await _publisher.CreateConnection();
            Console.WriteLine("Connected to message broker successfully!");

            // Start publishing messages
            _publishTask = Task.Run(() => PublishMessagesAsync(_cts.Token), cancellationToken);
        }
        catch (Publisher.Outbound.Exceptions.PublisherException ex)
        {
            Console.WriteLine($"\n❌ Failed to connect to message broker: {ex.Message}");
            if (ex.InnerException is System.Net.Sockets.SocketException socketEx)
            {
                Console.WriteLine($"   Socket error: {socketEx.Message}");
                Console.WriteLine($"   Make sure MessageBroker is running on the configured host and port.");
            }
            throw;
        }
    }

    /// <summary>
    /// Stops the publishing service.
    /// </summary>
    public async Task StopAsync()
    {
        Console.WriteLine("\nStopping Message Publisher Service...");

        await _cts.CancelAsync();

        if (_publishTask != null)
        {
            try
            {
                await _publishTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        Console.WriteLine($"Service stopped. Messages sent: {_messagesSent}, Failed: {_messagesFailed}");
    }

    private async Task PublishMessagesAsync(CancellationToken cancellationToken)
    {
        var messageNumber = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Create a demo message with reasonable content length
                var content = $"{_options.MessagePrefix} message #{messageNumber}";
                // Ensure content is not too long (max 500 chars to avoid serialization issues)
                if (content.Length > 500)
                {
                    content = content.Substring(0, 500);
                }
                
                var message = new DemoMessage
                {
                    Id = ++messageNumber,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = content,
                    Source = "PubSubDemo",
                    MessageType = GetRandomMessageType()
                };

                // Publish to broker (publisher handles serialization)
                try
                {
                    await _publisher.PublishAsync((T)(object)message);
                    Interlocked.Increment(ref _messagesSent);
                }
                catch (Exception publishEx)
                {
                    Interlocked.Increment(ref _messagesFailed);
                    Console.WriteLine($"\n❌ Failed to publish message #{messageNumber}: {publishEx.GetType().Name} - {publishEx.Message}");
                    if (publishEx.InnerException != null)
                    {
                        Console.WriteLine($"   Inner: {publishEx.InnerException.Message}");
                    }
                    throw; // Re-throw to be caught by outer catch
                }

                // Console output with status
                if (messageNumber % 10 == 0 || messageNumber <= 5)
                {
                    Console.WriteLine($"\n✅ Published message #{messageNumber}: {message.Content} (Type: {message.MessageType})");
                }
                else
                {
                    Console.Write(
                        $"\r[{DateTime.Now:HH:mm:ss}] Sent: {_messagesSent} | Failed: {_messagesFailed} | Last: {message.MessageType,-15}");
                }

                // Wait before sending next message
                await Task.Delay(_options.MessageInterval, cancellationToken);

                // Occasionally send a batch
                if (messageNumber % 50 == 0)
                {
                    await SendBatchAsync(messageNumber, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
                break;
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _messagesFailed);
                Console.WriteLine($"\n❌ Error publishing message #{messageNumber}: {ex.GetType().Name} - {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner: {ex.InnerException.Message}");
                }

                // Wait a bit before retrying, but don't wait too long
                var delayMs = (int)Math.Min(5000, 1000 * Math.Min(_messagesFailed, 5));
                await Task.Delay(delayMs, cancellationToken);
            }
        }
    }

    private async Task SendBatchAsync(int startNumber, CancellationToken cancellationToken)
    {
        Console.WriteLine($"\n\n📦 Sending batch of {_options.BatchSize} messages...");

        for (var i = 0; i < _options.BatchSize; i++)
        {
            try
            {
                var batchContent = $"BATCH message #{i + 1}/{_options.BatchSize}";
                // Ensure content is not too long
                if (batchContent.Length > 500)
                {
                    batchContent = batchContent.Substring(0, 500);
                }
                
                var message = new DemoMessage
                {
                    Id = startNumber + i + 1,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = batchContent,
                    Source = "PubSubDemo-Batch",
                    MessageType = "Batch"
                };

                await _publisher.PublishAsync((T)(object)message);
                Interlocked.Increment(ref _messagesSent);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _messagesFailed);
                Console.WriteLine($"Batch message {i + 1} failed: {ex.Message}");
            }
        }

        Console.WriteLine($"✅ Batch complete!\n");
    }

    private static string GetRandomMessageType()
    {
        var types = new[] { "Info", "Warning", "Error", "Debug", "Trace", "Event", "Metric", "Alert" };
        return types[Random.Shared.Next(types.Length)];
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cts.Dispose();

        if (_publisher is IAsyncDisposable disposable)
        {
            await disposable.DisposeAsync();
        }
    }
}

/// <summary>
/// Demo message structure.
/// </summary>
public sealed class DemoMessage
{
    public DemoMessage()
    {
        // Required for new() constraint
    }

    public int Id { get; set; }
    public long Timestamp { get; set; }
    public string Content { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public string MessageType { get; set; } = string.Empty;
}