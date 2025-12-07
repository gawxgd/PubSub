using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;

namespace MessageBroker.Inbound.TcpServer.Service;

public abstract class TcpServer(ConnectionType connectionType, int port, CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager)
    : BackgroundService
{
    private readonly Socket _socket = createSocketUseCase.CreateSocket(port);
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpServer>(LogSource.MessageBroker);

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo("TCP Server started listening");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var acceptedSocket = await _socket.AcceptAsync(cancellationToken);
                Logger.LogInfo($"Accepted client: {acceptedSocket.RemoteEndPoint}");

                var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                connectionManager.RegisterConnection(connectionType, acceptedSocket, linkedTokenSource);
            }
            catch (SocketException ex)
            {
                Logger.LogError($"Socket error: {ex.Message}", ex);
            }
            catch (OperationCanceledException)
            {
                Logger.LogInfo("Server shutdown requested");
                break;
            }
            catch (Exception ex)
            {
                Logger.LogError($"Unexpected error in ExecuteAsync: {ex.Message}", ex);
            }
        }
        
        Logger.LogInfo("TCP Server execution completed");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        Logger.LogInfo("Stopping TCP Server");
        
        await base.StopAsync(cancellationToken);
        await Task.Delay(100);

        try
        {
            await connectionManager.UnregisterAllConnectionsAsync();
            Logger.LogInfo("All connections unregistered");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Exception during connection cleanup: {ex.Message}", ex);
        }

        try
        {
            _socket.Shutdown(SocketShutdown.Receive);
            Logger.LogInfo("Socket shutdown initiated - stopped accepting new connections");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Exception during socket shutdown: {ex.Message}", ex);
        }

        try
        {
            _socket.Close();
            Logger.LogInfo("Socket closed and port released");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Exception during socket cleanup: {ex.Message}", ex);
        }
    }

    public override void Dispose()
    {
        _socket?.Dispose();
        base.Dispose();
    }
}