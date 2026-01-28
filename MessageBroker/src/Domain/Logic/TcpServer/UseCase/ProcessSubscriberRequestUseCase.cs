using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessSubscriberRequestUseCase(
    ICommitLogFactory commitLogFactory,
    ISubscriberDeliveryMetrics subscriberDeliveryMetrics) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessSubscriberRequestUseCase>(LogSource.MessageBroker);

    private readonly TopicOffsetDeformatter _deformatter = new();

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        var parsedMessage = ParseMessage(message);

        if (parsedMessage == null)
        {
            return;
        }

        var (topic, offset) = parsedMessage.Value;

        Logger.LogDebug($"Processing subscriber request: topic={topic}, offset={offset}");

        try
        {
            var commitLogReader = commitLogFactory.GetReader(topic);

            var batch = commitLogReader.ReadBatchBytes(offset);

            if (batch == null)
            {
                Logger.LogDebug($"No more batches available at offset {offset}");
            }
            else
            {
                var (batchBytes, batchOffset, lastOffset) = batch.Value;

                Logger.LogDebug(
                    $"Read batch with offset {batchOffset} and last offset {lastOffset} (requested: {offset})");

                await SendAllAsync(socket, batchBytes, cancellationToken);
                subscriberDeliveryMetrics.RecordBatchSent(topic, batchOffset, lastOffset);

                Logger.LogDebug(
                    $"Send batch with offset {batchOffset} and last offset {lastOffset}");
            }
        }
        catch (FileNotFoundException)
        {
            Logger.LogDebug($"No commit log found for topic '{topic}' - topic may be empty");
        }
        catch (SocketException ex)
        {
            Logger.LogError($"Socket error while sending to subscriber: {ex.Message}", ex);
        }
        catch (OperationCanceledException)
        {
            Logger.LogInfo("Subscriber connection cancelled");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error in subscriber processing: {ex.Message}", ex);
        }

        Logger.LogInfo("Messages from commit log sent to subscriber");
    }

    private static async Task SendAllAsync(Socket socket, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        var totalSent = 0;

        while (totalSent < data.Length)
        {
            var sent = await socket.SendAsync(data.Slice(totalSent), SocketFlags.None, cancellationToken);
            if (sent <= 0)
            {
                throw new SocketException((int)SocketError.ConnectionReset);
            }

            totalSent += sent;
        }
    }

    private (string topic, ulong offset)? ParseMessage(ReadOnlyMemory<byte> message)
    {
        var topicOffset = _deformatter.Deformat(message);
        if (topicOffset == null)
        {
            Logger.LogWarning("Failed to parse subscriber request returning null");
            return null;
        }

        return (topicOffset.Topic, topicOffset.Offset);
    }
}