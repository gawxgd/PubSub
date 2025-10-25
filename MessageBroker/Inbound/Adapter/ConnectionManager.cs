using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.Repositories;

namespace MessageBroker.Inbound.Adapter;

public class ConnectionManager(IConnectionRepository connectionRepository) : IConnectionManager
{
    private readonly IAutoLogger _logger = AutoLoggerFactory.CreateLogger<ConnectionManager>(LogSource.MessageBroker);

    public void RegisterConnection(Socket acceptedSocket, CancellationTokenSource cancellationTokenSource)
    {
        var connectionId = connectionRepository.GenerateConnectionId();

        _logger.LogInfo($"Registering connection from {acceptedSocket.RemoteEndPoint}");

        var handlerTask = Task.Run(() =>
        {
            _logger.LogDebug(
                $"Started handler thread for connection {connectionId} with client: {acceptedSocket.RemoteEndPoint}");
            return new HandleClientConnectionUseCase(acceptedSocket,
                    () => UnregisterConnectionAfterThreadFinish(connectionId))
                .HandleConnection(cancellationTokenSource.Token);
        }, cancellationTokenSource.Token);

        var connection = new Connection(connectionId, acceptedSocket.RemoteEndPoint?.ToString() ?? "Unknown",
            cancellationTokenSource, handlerTask);
        connectionRepository.Add(connection);

        _logger.LogInfo($"Connection registered with ID: {connectionId}");
    }

    public async Task UnregisterConnectionAsync(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            _logger.LogWarning($"Connection with id {connectionId} was not found");
            return;
        }

        await connection.DisconnectAsync();
        connection.Dispose();
        connectionRepository.Remove(connectionId);
        _logger.LogInfo($"Unregistered connection with ID: {connectionId}");
    }

    public async Task UnregisterAllConnectionsAsync()
    {
        var connections = connectionRepository.GetAll();
        _logger.LogInfo($"Unregistering {connections.Count} connections");

        foreach (var connection in connections)
        {
            await connection.DisconnectAsync();
            connection.Dispose();
        }

        connectionRepository.RemoveAll();
        _logger.LogInfo("All connections unregistered");
    }

    private void UnregisterConnectionAfterThreadFinish(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            _logger.LogWarning($"Connection with id {connectionId} was not found");
            return;
        }


        connection.Dispose();
        connectionRepository.Remove(connectionId);
        _logger.LogInfo($"Unregistered connection with ID {connectionId} after thread finished");
    }
}