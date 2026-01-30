using System;
using System.IO;
using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedPublisherMessageUseCase(
    ICommitLogFactory commitLogFactory,
    SendPublishResponseUseCase sendPublishResponseUseCase) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedPublisherMessageUseCase>(LogSource.MessageBroker);

    private readonly MessageWithTopicDeformatter _deformatter = new();

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        Logger.LogInfo($"Processing publisher message: {message.Length} bytes");
        
        try
        {
            var (topic, batchBytes) = ParseMessage(message);

            Logger.LogInfo($"Parsed message for topic '{topic}', batch size: {batchBytes.Length} bytes");

            var commitLogAppender = commitLogFactory.GetAppender(topic);
            var baseOffset = await commitLogAppender.AppendAsync(batchBytes);

            var response = new PublishResponse(baseOffset, ErrorCode.None);
            await sendPublishResponseUseCase.SendResponseAsync(socket, response, cancellationToken);

            Logger.LogInfo(
                $"Appended batch ({batchBytes.Length} bytes) to commit log for topic '{topic}', baseOffset={baseOffset}");
        }
        catch (InvalidDataException ex)
        {
            Logger.LogError($"Failed to parse message: {ex.Message}", ex);
            var errorResponse = new PublishResponse(0, ErrorCode.InvalidMessageFormat);
            await sendPublishResponseUseCase.SendResponseAsync(socket, errorResponse, cancellationToken);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("not configured"))
        {
            Logger.LogError($"Topic not found: {ex.Message}", ex);
            var errorResponse = new PublishResponse(0, ErrorCode.TopicNotFound);
            await sendPublishResponseUseCase.SendResponseAsync(socket, errorResponse, cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Internal error while processing message: {ex.Message}", ex);
            var errorResponse = new PublishResponse(0, ErrorCode.InternalError);
            await sendPublishResponseUseCase.SendResponseAsync(socket, errorResponse, cancellationToken);
        }
    }

    private (string topic, byte[] batchBytes) ParseMessage(ReadOnlyMemory<byte> message)
    {
        Logger.LogDebug($"ParseMessage: received {message.Length} bytes, first 50 bytes: {Convert.ToHexString(message.Span.Slice(0, Math.Min(50, message.Length)))}");
        
        var messageWithTopic = _deformatter.Deformat(message);
        if (messageWithTopic == null)
        {
            throw new InvalidDataException($"Invalid publisher message format: failed to parse message ({message.Length} bytes)");
        }

        return (messageWithTopic.Topic, messageWithTopic.Payload);
    }
}
