using System.Text;
using System.Threading.Channels;
using Subscriber.Domain;

namespace Subscriber.Inbound.Adapter;

public sealed class TcpSubscriber(
    string topic,
    int minMessageLength,
    int maxMessageLength,
    TimeSpan pollInterval,
    uint maxRetryAttempts,
    ISubscriberConnection connection,
    Func<string, Task> messageHandler,
    Func<Exception, Task>? errorHandler = null)
    : ISubscriber, IAsyncDisposable
{
    private readonly Channel<byte[]> _inboundChannel = Channel.CreateUnbounded<byte[]>();
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public async Task CreateConnection(CancellationToken cancellationToken)
    {
        var retryCount = 0;

        while (retryCount < maxRetryAttempts && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                await connection.ConnectAsync(cancellationToken);
                return;
            }
            catch (SubscriberConnectionException ex)
            {
                retryCount++;
                var delay = TimeSpan.FromSeconds(Math.Min(retryCount, maxRetryAttempts));
                Console.WriteLine($"[Subscriber] Retry {retryCount}: {ex.Message}. Waiting {delay}...");
                await Task.Delay(delay, cancellationToken);
            }
        }

        throw new SubscriberConnectionException("Max retry attempts exceeded", null);
    }

    public async Task ReceiveAsync(byte[] message, CancellationToken cancellationToken)
    {
        var text = Encoding.UTF8.GetString(message);

        if (text.Length < minMessageLength || text.Length > maxMessageLength)
        {
            Console.WriteLine($"[Subscriber] Invalid message length: {text.Length}");
            return;
        }

        if (!text.StartsWith($"{topic}:"))
        {
            Console.WriteLine($"[Subscriber] Ignored message (wrong topic): {text}");
            return;
        }

        var payload = text.Substring(topic.Length + 1);

        try
        {
            await messageHandler(payload);
        }
        catch (Exception ex)
        {
            if (errorHandler != null)
                await errorHandler(ex);
            else
                Console.WriteLine($"[Subscriber] Handler error: {ex.Message}");
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await CreateConnection(cancellationToken);

        while (!_cancellationTokenSource.Token.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (await _inboundChannel.Reader.WaitToReadAsync(cancellationToken))
                {
                    while (_inboundChannel.Reader.TryRead(out var message))
                    {
                        await ReceiveAsync(message, cancellationToken);
                    }
                }
            }
            catch (Exception ex)
            {
                if (errorHandler != null)
                    await errorHandler(ex);
                else
                    Console.WriteLine($"[Subscriber] Error: {ex.Message}");
            }

            await Task.Delay(pollInterval, cancellationToken);
        }

        await connection.DisconnectAsync(_cancellationTokenSource.Token);
    }

    public ChannelWriter<byte[]> GetChannelWriter() => _inboundChannel.Writer;

    public async ValueTask DisposeAsync()
    {
        await _cancellationTokenSource.CancelAsync();
        _inboundChannel.Writer.TryComplete();
        await _inboundChannel.Reader.Completion;
        await connection.DisconnectAsync(_cancellationTokenSource.Token);
        _cancellationTokenSource.Dispose();
    }
}
