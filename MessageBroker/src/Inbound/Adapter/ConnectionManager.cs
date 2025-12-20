using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;

namespace MessageBroker.Inbound.Adapter;

public class ConnectionManager(
    IConnectionRepository connectionRepository,
    ICommitLogFactory commitLogFactory,
    ILogRecordBatchReader batchReader)
    : IConnectionManager
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ConnectionManager>(LogSource.MessageBroker);

    public void RegisterConnection(ConnectionType connectionType, Socket acceptedSocket,
        CancellationTokenSource cancellationTokenSource)
    {
        var connectionId = connectionRepository.GenerateConnectionId();

        Logger.LogInfo($"Registering connection from {acceptedSocket.RemoteEndPoint}");

        var handlerTask = Task.Run(() =>
        {
            Logger.LogDebug(
                $"Started handler thread for connection {connectionId} with client: {acceptedSocket.RemoteEndPoint}");
            IMessageProcessorUseCase messageProcessorUseCase = connectionType switch
            {
                ConnectionType.Publisher => new ProcessReceivedPublisherMessageUseCase(commitLogFactory, "default",
                    batchReader),
                ConnectionType.Subscriber => new ProcessSubscriberRequestUseCase(commitLogFactory),
                _ => throw new ArgumentOutOfRangeException(nameof(connectionType), connectionType, null),
            };

            IHandleClientConnectionUseCase handleClientConnectionUseCase = new HandleClientConnectionUseCase(
                acceptedSocket,
                () => UnregisterConnectionAfterThreadFinish(connectionId),
                messageProcessorUseCase,
                commitLogFactory,
                batchReader);

            return handleClientConnectionUseCase.HandleConnection(cancellationTokenSource.Token);
        }, cancellationTokenSource.Token);

        var connection = new Connection(
            connectionId,
            acceptedSocket.RemoteEndPoint?.ToString() ?? "Unknown",
            cancellationTokenSource,
            handlerTask,
            connectionType);
        connectionRepository.Add(connection);

        Logger.LogInfo($"Connection registered with ID: {connectionId}");
    }

    public async Task UnregisterConnectionAsync(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            Logger.LogWarning($"Connection with id {connectionId} was not found");
            return;
        }

        await connection.DisconnectAsync();
        connection.Dispose();
        connectionRepository.Remove(connectionId);
        Logger.LogInfo($"Unregistered connection with ID: {connectionId}");
    }

    public async Task UnregisterAllConnectionsAsync()
    {
        var connections = connectionRepository.GetAll();
        Logger.LogInfo($"Unregistering {connections.Count} connections");

        foreach (var connection in connections)
        {
            await connection.DisconnectAsync();
            connection.Dispose();
        }

        connectionRepository.RemoveAll();
        Logger.LogInfo("All connections unregistered");
    }

    private void UnregisterConnectionAfterThreadFinish(long connectionId)
    {
        var connection = connectionRepository.Get(connectionId);
        if (connection == null)
        {
            Logger.LogWarning($"Connection with id {connectionId} was not found");
            return;
        }

        connection.Dispose();
        connectionRepository.Remove(connectionId);
        Logger.LogInfo($"Unregistered connection with ID {connectionId} after thread finished");
    }
}