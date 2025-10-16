using System.Net.Sockets;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.Repositories;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Inbound.Adapter;

public class ConnectionManager(IConnectionRepository connectionRepository, ILogger<ConnectionManager> logger, ILoggerFactory loggerFactory) : IConnectionManager
{
    public void RegisterConnection(Socket acceptedSocket, CancellationTokenSource cancellationTokenSource)
    {
        var connectionId = connectionRepository.GenerateConnectionId();
        var handlerTask = Task.Run(() =>
        {
            logger.LogInformation("Started new thread for client: {Endpoint}", acceptedSocket.RemoteEndPoint);
            var connectionLogger = loggerFactory.CreateLogger<HandleClientConnectionUseCase>();
            var processLogger = loggerFactory.CreateLogger<ProcessReceivedMessageUseCase>();
            return new HandleClientConnectionUseCase(acceptedSocket,
                    () => UnregisterConnectionAfterThreadFinish(connectionId),
                    connectionLogger,
                    processLogger)
                .HandleConnection(cancellationTokenSource.Token);
        }, cancellationTokenSource.Token);

        var connection = new Connection(connectionId, acceptedSocket.RemoteEndPoint?.ToString() ?? "Unknown",
            cancellationTokenSource, handlerTask, loggerFactory.CreateLogger<Connection>());
        connectionRepository.Add(connection);

        logger.LogInformation("Registered new connection with ID: {ConnectionId}", connectionId);
    }

    public async Task UnregisterConnectionAsync(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            logger.LogWarning("Connection with id {ConnectionId} was not found", connectionId);
            return;
        }

        await connection.DisconnectAsync();
        connection.Dispose();
        connectionRepository.Remove(connectionId);

        logger.LogInformation("Unregistered connection with ID: {ConnectionId}", connectionId);
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
        logger.LogInformation("Unregistered all connections");
    }

    private void UnregisterConnectionAfterThreadFinish(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            logger.LogWarning("Connection with id {ConnectionId} was not found", connectionId);
            return;
        }

        connection.Dispose();
        connectionRepository.Remove(connectionId);

        logger.LogInformation("Unregistered connection with ID after thread finished: {ConnectionId}", connectionId);
    }
}