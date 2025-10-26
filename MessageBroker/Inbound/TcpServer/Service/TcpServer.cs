using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using ILogger = LoggerLib.Domain.Port.ILogger;

namespace MessageBroker.Inbound.TcpServer.Service;

public class TcpServer(CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager, ILogger logger)
    : BackgroundService
{
    private readonly Socket _socket = createSocketUseCase.CreateSocket();

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var acceptedSocket = await _socket.AcceptAsync(cancellationToken);
                logger.LogInfo(LogSource.MessageBroker, $"Accepted client: {acceptedSocket.RemoteEndPoint}");

                var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                connectionManager.RegisterConnection(acceptedSocket, linkedTokenSource);
            }
            catch (SocketException ex)
            {
                logger.LogError(LogSource.MessageBroker, $"Socket error: {ex.Message}");
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // First stop accepting new connections
        await base.StopAsync(cancellationToken);

        await Task.Delay(100);

        try
        {
            await connectionManager.UnregisterAllConnectionsAsync();
            logger.LogInfo(LogSource.MessageBroker, "Server stopped unregistered all connections");
        }
        catch (Exception ex)
        {
            logger.LogError(LogSource.MessageBroker, $"Exception during connection cleanup: {ex.Message}");
        }
    }

    public override void Dispose()
    {
        _socket?.Dispose();
        base.Dispose();
    }
}