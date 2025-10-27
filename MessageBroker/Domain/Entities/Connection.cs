using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace MessageBroker.Domain.Entities;

public class Connection(
    long id,
    string clientEndpoint,
    CancellationTokenSource cancellationTokenSource,
    Task handlerTask) : IDisposable
{
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<Connection>(LogSource.MessageBroker);

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
            Logger.LogWarning($"Connection with id {Id} has been already disposed.");
            return;
        }

        CancellationTokenSource.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
        Logger.LogWarning($"Connection with id {Id} has been disposed.");
    }

    public async Task DisconnectAsync()
    {
        if (_disposed || CancellationTokenSource.IsCancellationRequested)
        {
            Logger.LogWarning($"Connection with id {Id} has been already disconnected / disposed.");
            return;
        }

        await CancellationTokenSource.CancelAsync();

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await HandlerTask.WaitAsync(timeoutCts.Token);
            Logger.LogInfo($"Connection with id {Id} has been disconnected.");
        }
        catch (OperationCanceledException ex)
        {
            // Timeout - handler didn't complete in time
            Logger.LogWarning($"Connection with id {Id} disconnection timed out", ex);
        }
    }
}