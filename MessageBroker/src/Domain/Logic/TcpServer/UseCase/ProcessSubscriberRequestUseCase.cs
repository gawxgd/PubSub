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
            ulong currentOffset = offset;
            var batchesSent = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                var batch = commitLogReader.ReadBatchBytes(currentOffset);

                if (batch == null)
                {
                    if (batchesSent == 0)
                    {
                        Logger.LogDebug($"No batches available at offset {currentOffset} for topic '{topic}' - topic may be empty or offset is beyond available data");
                    }
                    else
                    {
                        Logger.LogDebug($"No more batches available at offset {currentOffset} - sent {batchesSent} batches total");
                    }
                    break;
                }

                var (batchBytes, batchOffset, lastOffset) = batch.Value;

                Logger.LogDebug(
                    $"Read batch with offset {batchOffset} and last offset {lastOffset}, size: {batchBytes.Length} bytes");

                try
                {
                    // Send batch bytes directly - no framing needed for subscriber responses
                    var bytesSent = await socket.SendAsync(batchBytes, SocketFlags.None, cancellationToken);
                    
                    if (bytesSent != batchBytes.Length)
                    {
                        Logger.LogWarning(
                            $"Partial send: sent {bytesSent} of {batchBytes.Length} bytes for batch {batchOffset}-{lastOffset}");
                    }
                    
                    batchesSent++;
                    Logger.LogInfo(
                        $"✅ Sent batch {batchesSent} to subscriber: offset {batchOffset} to {lastOffset}, {bytesSent}/{batchBytes.Length} bytes");
                    
                    // Small delay to ensure data is flushed
                    await Task.Delay(10, cancellationToken);
                }
                catch (SocketException ex)
                {
                    Logger.LogError($"❌ Failed to send batch to subscriber: {ex.Message} (SocketError: {ex.SocketErrorCode})");
                    throw;
                }

                currentOffset = lastOffset + 1;
            }

            if (batchesSent > 0)
            {
                Logger.LogInfo($"Successfully sent {batchesSent} batches to subscriber for topic '{topic}'");
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