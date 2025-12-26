using System.IO;
using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedPublisherMessageUseCase(
    ICommitLogFactory commitLogFactory) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedPublisherMessageUseCase>(LogSource.MessageBroker);

    private readonly MessageWithTopicDeformatter _deformatter = new();

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        Logger.LogDebug($"Processing publisher message: {message.Length} bytes");

        try
        {
            var (topic, batchBytes) = ParseMessage(message);
            
            Logger.LogDebug($"Parsed message for topic '{topic}', batch size: {batchBytes.Length} bytes");

            var commitLogAppender = commitLogFactory.GetAppender(topic);
            await commitLogAppender.AppendAsync(batchBytes);

            Logger.LogInfo($"Appended batch ({batchBytes.Length} bytes) to commit log for topic '{topic}'");
        }
        catch (InvalidDataException ex)
        {
            Logger.LogError($"Failed to parse message: {ex.Message}", ex);
            throw;
        }
    }

    private (string topic, byte[] batchBytes) ParseMessage(ReadOnlyMemory<byte> message)
    {
        var messageWithTopic = _deformatter.Deformat(message);
        if (messageWithTopic == null)
        {
            throw new InvalidDataException("Invalid publisher message format: failed to parse message");
        }

        return (messageWithTopic.Topic, messageWithTopic.Payload);
    }
}
