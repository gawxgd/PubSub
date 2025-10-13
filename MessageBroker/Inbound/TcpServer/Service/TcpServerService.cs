using System.Net.Sockets;
using Microsoft.Extensions.Hosting;

namespace MessageBroker.TcpServer;

public class TcpServerService(CreateSocketUseCase createSocketUseCase) : BackgroundService
{
    private readonly Socket _socket = createSocketUseCase.CreateSocket();

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await AcceptConnectionsAsync(_socket, cancellationToken);
    }

    private async Task AcceptConnectionsAsync(Socket listenerSocket, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
            try
            {
                var acceptedSocket = await listenerSocket.AcceptAsync(cancellationToken);
                Console.WriteLine($"Accepted client: {acceptedSocket.RemoteEndPoint}");

                _ = Task.Run(() => HandleConnectionAsync(acceptedSocket, cancellationToken), cancellationToken);
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

    private async Task HandleConnectionAsync(Socket acceptedSocket, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Started new thread for handling connection with client: {acceptedSocket.RemoteEndPoint}");
        await new HandleClientConnectionUseCase(acceptedSocket).HandleConnection(cancellationToken);
    }

    public override void Dispose()
    {
        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception caught while shutting down server", ex);
        }

        _socket?.Dispose();
        base.Dispose();
    }
}