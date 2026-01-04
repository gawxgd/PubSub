using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessSubscriberRequestUseCase(
    ICommitLogFactory commitLogFactory,
    ILogRecordBatchWriter batchWriter) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessSubscriberRequestUseCase>(LogSource.MessageBroker);

    private readonly TopicOffsetDeformatter _deformatter = new();

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        var (topic, offset) = ParseMessage(message);

        Logger.LogDebug($"Processing subscriber request: topic={topic}, offset={offset}");

        ICommitLogReader? commitLogReader = null;

        try
        {
            commitLogReader = commitLogFactory.GetReader(topic);
            
            // Send only one batch per request to prevent duplicate processing
            var batch = commitLogReader.ReadBatchBytes(offset);

            if (batch == null)
            {
                Logger.LogDebug($"No more batches available at offset {offset}");
            }
            else
            {
                var (batchBytes, batchOffset, lastOffset) = batch.Value;

                Logger.LogDebug(
                    $"Read batch with offset {batchOffset} and last offset {lastOffset}");

                await socket.SendAsync(batchBytes, SocketFlags.None, cancellationToken);

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
        finally
        {
            if (commitLogReader != null)
                await commitLogReader.DisposeAsync();
        }

        Logger.LogInfo("Messages from commit log sent to subscriber");
    }

    private (string topic, ulong offset) ParseMessage(ReadOnlyMemory<byte> message)
    {
        var topicOffset = _deformatter.Deformat(message);
        if (topicOffset == null)
        {
            Logger.LogWarning("Failed to parse subscriber request, using defaults");
            topicOffset = new TopicOffset("default", 0);
        }

        return (topicOffset.Topic, topicOffset.Offset);
    }
}