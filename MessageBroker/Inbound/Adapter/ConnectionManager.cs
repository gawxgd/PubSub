using System.Net.Sockets;
using LoggerLib;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.Repositories;
using ILogger = LoggerLib.ILogger;

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
                    () => UnregisterConnectionAfterThreadFinish(connectionId),
                    logger)
                .HandleConnection(cancellationTokenSource.Token);
        }, cancellationTokenSource.Token);

        var connection = new Connection(connectionId, acceptedSocket.RemoteEndPoint?.ToString() ?? "Unknown",
            cancellationTokenSource, handlerTask, logger);
        connectionRepository.Add(connection);

        logger.LogInfo(LogSource.MessageBroker, $"New connection with ID: {connectionId}");
    }

    public async Task UnregisterConnectionAsync(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            logger.LogWarning(LogSource.MessageBroker, $"Connection with id {connectionId} was not found");
            return;
        }

        await connection.DisconnectAsync();
        connection.Dispose();
        connectionRepository.Remove(connectionId);

        logger.LogInfo(LogSource.MessageBroker,$"Unregistered connection with ID: {connectionId}");
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
        logger.LogInfo(LogSource.MessageBroker,$"Unregistered all connections");
    }

    private void UnregisterConnectionAfterThreadFinish(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            logger.LogWarning(LogSource.MessageBroker, $"Connection with id {connectionId} was not found");
            return;
        }

        connection.Dispose();
        connectionRepository.Remove(connectionId);

        logger.LogInfo(LogSource.MessageBroker, $"Unregistered connection with ID after thread finished: {connectionId}");
    }
}