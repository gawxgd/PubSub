using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port.Repositories;

namespace MessageBroker.Inbound.TcpServer.Service;

public class TcpServer(CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager)
    : BackgroundService
{
    private readonly IAutoLogger _logger = AutoLoggerFactory.CreateLogger<TcpServer>(LogSource.MessageBroker);
    private readonly Socket _socket = createSocketUseCase.CreateSocket();

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInfo("TCP Server started listening");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var acceptedSocket = await _socket.AcceptAsync(cancellationToken);
                _logger.LogInfo($"Accepted client: {acceptedSocket.RemoteEndPoint}");

                var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                connectionManager.RegisterConnection(acceptedSocket, linkedTokenSource);
            }
            catch (SocketException ex)
            {
                _logger.LogError($"Socket error: {ex.Message}", ex);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInfo("Server shutdown requested");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Unexpected error in ExecuteAsync: {ex.Message}", ex);
            }
        }

        _logger.LogInfo("TCP Server execution completed");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInfo("Stopping TCP Server");

        await base.StopAsync(cancellationToken);
        await Task.Delay(100);

        try
        {
            await connectionManager.UnregisterAllConnectionsAsync();
            _logger.LogInfo("All connections unregistered");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Exception during connection cleanup: {ex.Message}", ex);
        }

        try
        {
            _socket.Shutdown(SocketShutdown.Receive);
            _logger.LogInfo("Socket shutdown initiated - stopped accepting new connections");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Exception during socket shutdown: {ex.Message}", ex);
        }

        try
        {
            _socket.Close();
            _logger.LogInfo("Socket closed and port released");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Exception during socket cleanup: {ex.Message}", ex);
        }
    }

    public override void Dispose()
    {
        _socket?.Dispose();
        base.Dispose();
    }
}