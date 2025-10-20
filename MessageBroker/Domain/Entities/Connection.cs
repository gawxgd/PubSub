using LoggerLib;

namespace MessageBroker.Domain.Entities;

public class Connection(
    long id,
    string clientEndpoint,
    CancellationTokenSource cancellationTokenSource,
    Task handlerTask,
    ILogger logger) : IDisposable
{
    private bool _disposed;
    public long Id { get; } = id;
    public string ClientEndpoint { get; } = clientEndpoint;
    public DateTime ConnectedAt { get; } = DateTime.UtcNow;
    public Task HandlerTask { get; } = handlerTask;
    public CancellationTokenSource CancellationTokenSource { get; } = cancellationTokenSource;

    public void Dispose()
    {
        if (_disposed)
        {
            logger.LogWarning(LogSource.MessageBroker,$"Connection with id {Id} has been already disposed.");
            return;
        }

        CancellationTokenSource.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
        Console.WriteLine($"Connection with id {Id} has been disposed.");
    }

    public async Task DisconnectAsync()
    {
        if (_disposed || CancellationTokenSource.IsCancellationRequested)
        {
            Console.WriteLine($"Connection with id {Id} has been already disconnected / disposed.");
            return;
        }

        await CancellationTokenSource.CancelAsync();

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await HandlerTask.WaitAsync(timeoutCts.Token);
            Console.WriteLine($"Connection with id {Id} has been disconnected.");
        }
        catch (OperationCanceledException)
        {
            // Timeout - handler didn't complete in time
            Console.WriteLine($"Connection {Id} handler did not complete within timeout");
        }
    }
}