using Microsoft.Extensions.Logging;

namespace MessageBroker.Domain.Entities;

public class Connection(
    long id,
    string clientEndpoint,
    CancellationTokenSource cancellationTokenSource,
    Task handlerTask,
    ILogger<Connection>? logger = null) : IDisposable
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
            logger?.LogWarning("Connection with id {Id} has been already disposed.", Id);
            return;
        }

        CancellationTokenSource.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
        logger?.LogInformation("Connection with id {Id} has been disposed.", Id);
    }

    public async Task DisconnectAsync()
    {
        if (_disposed || CancellationTokenSource.IsCancellationRequested)
        {
            logger?.LogWarning("Connection with id {Id} has been already disconnected / disposed.", Id);
            return;
        }

        await CancellationTokenSource.CancelAsync();

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await HandlerTask.WaitAsync(timeoutCts.Token);
            logger?.LogInformation("Connection with id {Id} has been disconnected.", Id);
        }
        catch (OperationCanceledException)
        {
            // Timeout - handler didn't complete in time
            logger?.LogWarning("Connection {Id} handler did not complete within timeout", Id);
        }
    }
}