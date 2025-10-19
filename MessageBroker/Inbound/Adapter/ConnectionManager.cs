using System.Net.Sockets;
using LoggerLib;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.Repositories;

namespace MessageBroker.Inbound.Adapter;

public class ConnectionManager(IConnectionRepository connectionRepository, ILogger logger) : IConnectionManager
{
    public void RegisterConnection(Socket acceptedSocket, CancellationTokenSource cancellationTokenSource)
    {
        var connectionId = connectionRepository.GenerateConnectionId();
        var handlerTask = Task.Run(() =>
        {
            Console.WriteLine(
                $"Started new thread for handling connection with client: {acceptedSocket.RemoteEndPoint}");
            return new HandleClientConnectionUseCase(acceptedSocket,
                    () => UnregisterConnectionAfterThreadFinish(connectionId))
                .HandleConnection(cancellationTokenSource.Token);
        }, cancellationTokenSource.Token);

        var connection = new Connection(connectionId, acceptedSocket.RemoteEndPoint?.ToString() ?? "Unknown",
            cancellationTokenSource, handlerTask, logger);
        connectionRepository.Add(connection);

        Console.WriteLine($"Registered new connection with ID: {connectionId}");
    }

    public async Task UnregisterConnectionAsync(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            Console.WriteLine($"Connection with id {connectionId} was not found");
            return;
        }

        await connection.DisconnectAsync();
        connection.Dispose();
        connectionRepository.Remove(connectionId);

        Console.WriteLine($"Unregistered connection with ID: {connectionId}");
    }

    public async Task UnregisterAllConnectionsAsync()
    {
        var connections = connectionRepository.GetAll();

        foreach (var connection in connections)
        {
            await connection.DisconnectAsync();
            connection.Dispose();
        }

        connectionRepository.RemoveAll();
        Console.WriteLine("Unregistered all connections");
    }

    private void UnregisterConnectionAfterThreadFinish(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            Console.WriteLine($"Connection with id {connectionId} was not found");
            return;
        }

        connection.Dispose();
        connectionRepository.Remove(connectionId);

        Console.WriteLine($"Unregistered connection with ID after thread finished: {connectionId}");
    }
}