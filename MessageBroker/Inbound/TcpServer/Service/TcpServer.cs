using System.Net.Sockets;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port.Repositories;
using Microsoft.Extensions.Hosting;

namespace MessageBroker.Inbound.TcpServer.Service;

public class TcpServer(CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager)
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
                Console.WriteLine($"Accepted client: {acceptedSocket.RemoteEndPoint}");

                var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                connectionManager.RegisterConnection(acceptedSocket, linkedTokenSource);
            }
            catch (SocketException ex)
            {
                Console.WriteLine($"Socket error: {ex.Message}");
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
            Console.WriteLine("Server stopped unregistered all connections");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception during connection cleanup: {ex.Message}");
        }
    }

    public override void Dispose()
    {
        _socket?.Dispose();
        base.Dispose();
    }
}