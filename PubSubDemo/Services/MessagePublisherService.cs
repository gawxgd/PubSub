using Publisher.Domain.Port;
using PubSubDemo.Configuration;

namespace PubSubDemo.Services;

public sealed class MessagePublisherService<T> : IAsyncDisposable
{
    private readonly (string Topic, IPublisher<T> Publisher)[] _publishers;
    private readonly DemoOptions _options;
    private readonly CancellationTokenSource _cts;
    private Task? _publishTask;
    private long _messagesSent;
    private long _messagesFailed;

    public MessagePublisherService(
        IEnumerable<(string Topic, IPublisher<T> Publisher)> publishers,
        DemoOptions options)
    {
        ArgumentNullException.ThrowIfNull(publishers);
        _publishers = publishers.ToArray();
        if (_publishers.Length == 0)
        {
            throw new ArgumentException("At least one publisher must be provided.", nameof(publishers));
        }
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _cts = new CancellationTokenSource();
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Starting Message Publisher Service...");

        try
        {
            foreach (var (topic, publisher) in _publishers)
            {
                await publisher.CreateConnection();
                Console.WriteLine($"Connected to message broker successfully for topic '{topic}'!");
            }

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
                var publisherIndex = messageNumber % _publishers.Length;
                var (topic, publisher) = _publishers[publisherIndex];

                var content = $"{_options.MessagePrefix} message #{messageNumber}";
                if (content.Length > 500)
                {
                    content = content.Substring(0, 500);
                }
                
                var message = new DemoMessage
                {
                    Id = ++messageNumber,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = content,
                    Source = $"PubSubDemo[{topic}]",
                    MessageType = GetRandomMessageType()
                };

                try
                {
                    await publisher.PublishAsync((T)(object)message);
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
                    throw;
                }

                if (messageNumber % 10 == 0 || messageNumber <= 5)
                {
                    Console.WriteLine($"\n✅ Published message #{messageNumber}: {message.Content} (Type: {message.MessageType})");
                }
                else
                {
                    Console.Write(
                        $"\r[{DateTime.Now:HH:mm:ss}] Sent: {_messagesSent} | Failed: {_messagesFailed} | Last: {message.MessageType,-15}");
                }

                await Task.Delay(_options.MessageInterval, cancellationToken);

                if (messageNumber % 50 == 0)
                {
                    await SendBatchAsync(topic, publisher, messageNumber, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
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

                var delayMs = (int)Math.Min(5000, 1000 * Math.Min(_messagesFailed, 5));
                await Task.Delay(delayMs, cancellationToken);
            }
        }
    }

    private async Task SendBatchAsync(string topic, IPublisher<T> publisher, int startNumber, CancellationToken cancellationToken)
    {
        Console.WriteLine($"\n\n📦 Sending batch of {_options.BatchSize} messages to topic '{topic}'...");

        for (var i = 0; i < _options.BatchSize; i++)
        {
            try
            {
                var batchContent = $"BATCH message #{i + 1}/{_options.BatchSize}";
                if (batchContent.Length > 500)
                {
                    batchContent = batchContent.Substring(0, 500);
                }
                
                var message = new DemoMessage
                {
                    Id = startNumber + i + 1,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = batchContent,
                    Source = $"PubSubDemo-Batch[{topic}]",
                    MessageType = "Batch"
                };

                await publisher.PublishAsync((T)(object)message);
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

        foreach (var (_, publisher) in _publishers)
        {
            if (publisher is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync();
            }
        }
    }
}

public sealed class DemoMessage
{
    public DemoMessage()
    {
    }

    public int Id { get; set; }
    public long Timestamp { get; set; }
    public string Content { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public string MessageType { get; set; } = string.Empty;
}
