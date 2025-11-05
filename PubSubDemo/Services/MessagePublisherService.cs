using System.Text;
using System.Text.Json;
using Publisher.Domain.Port;
using PubSubDemo.Configuration;

namespace PubSubDemo.Services;

/// <summary>
/// Background service that publishes demo messages to the broker.
/// </summary>
public sealed class MessagePublisherService : IAsyncDisposable
{
    private readonly IPublisher _publisher;
    private readonly DemoOptions _options;
    private readonly CancellationTokenSource _cts;
    private Task? _publishTask;
    private long _messagesSent;
    private long _messagesFailed;

    public MessagePublisherService(IPublisher publisher, DemoOptions options)
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
        
        // Connect to the broker
        await _publisher.CreateConnection();
        Console.WriteLine("Connected to message broker successfully!");

        // Start publishing messages
        _publishTask = Task.Run(() => PublishMessagesAsync(_cts.Token), cancellationToken);
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
                // Create a demo message
                var message = new DemoMessage
                {
                    Id = ++messageNumber,
                    Timestamp = DateTimeOffset.UtcNow,
                    Content = $"{_options.MessagePrefix} message #{messageNumber}",
                    Source = "PubSubDemo",
                    MessageType = GetRandomMessageType()
                };

                // Serialize to JSON
                var json = JsonSerializer.Serialize(message);
                var bytes = Encoding.UTF8.GetBytes(json);

                // Publish to broker
                await _publisher.PublishAsync(bytes);
                
                Interlocked.Increment(ref _messagesSent);

                // Console output with status
                Console.Write($"\r[{DateTime.Now:HH:mm:ss}] Sent: {_messagesSent} | Failed: {_messagesFailed} | Last: {message.MessageType,-15}");

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
                Console.WriteLine($"\nError publishing message: {ex.Message}");
                
                // Wait a bit before retrying
                await Task.Delay(1000, cancellationToken);
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
                var message = new DemoMessage
                {
                    Id = startNumber + i + 1,
                    Timestamp = DateTimeOffset.UtcNow,
                    Content = $"BATCH message #{i + 1}/{_options.BatchSize}",
                    Source = "PubSubDemo-Batch",
                    MessageType = "Batch"
                };

                var json = JsonSerializer.Serialize(message);
                var bytes = Encoding.UTF8.GetBytes(json);

                await _publisher.PublishAsync(bytes);
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
    public int Id { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public string Content { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public string MessageType { get; set; } = string.Empty;
}



