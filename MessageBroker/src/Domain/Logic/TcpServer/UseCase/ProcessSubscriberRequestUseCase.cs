using System.IO;
using System.Net.Sockets;
using System.Text;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

//ToDo update to send plain bytes
public class ProcessSubscriberRequestUseCase(
    ICommitLogFactory commitLogFactory,
    ILogRecordBatchWriter batchWriter) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessSubscriberRequestUseCase>(LogSource.MessageBroker);

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        var (topic, offset) = ParseRequest(message);

        Logger.LogDebug($"Processing subscriber request: topic={topic}, offset={offset}");

        ICommitLogReader? commitLogReader = null;

        try
        {
            commitLogReader = commitLogFactory.GetReader(topic);
            ulong currentOffset = offset;

            while (!cancellationToken.IsCancellationRequested)
            {
                var batch = commitLogReader.ReadRecordBatch(currentOffset);

                if (batch == null)
                {
                    Logger.LogDebug($"No more batches available at offset {currentOffset}");
                    break;
                }

                Logger.LogDebug(
                    $"Read batch with {batch.Records.Count} records, base offset: {batch.BaseOffset}, last offset: {batch.LastOffset}");


                {
                    // Serialize the batch to bytes
                    byte[] batchBytes;
                    using (var batchStream = new MemoryStream())
                    {
                        batchWriter.WriteTo(batch, batchStream);
                        batchBytes = batchStream.ToArray();
                    }

                    // Send framed message: [8-byte offset][4-byte length][serializedBatchBytes]
                    var baseOffsetBytes = BitConverter.GetBytes(batch.BaseOffset);
                    var lengthBytes = BitConverter.GetBytes(batchBytes.Length);

                    await socket.SendAsync(baseOffsetBytes, SocketFlags.None, cancellationToken);
                    await socket.SendAsync(lengthBytes, SocketFlags.None, cancellationToken);
                    await socket.SendAsync(batchBytes, SocketFlags.None, cancellationToken);

                    Logger.LogDebug(
                        $"Sent batch at base offset {batch.BaseOffset}: {batchBytes.Length} bytes (framed: {baseOffsetBytes.Length + lengthBytes.Length + batchBytes.Length} total)");
                }

                // Move to the next offset after this batch
                currentOffset = batch.LastOffset + 1;
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

    //ToDo move to seprate class
    private static (string topic, ulong offset) ParseRequest(ReadOnlyMemory<byte> message)
    {
        // Request format: "topic:offset\n"
        var requestString = Encoding.UTF8.GetString(message.Span).TrimEnd('\n', '\r');
        var parts = requestString.Split(':');

        if (parts.Length != 2)
        {
            Logger.LogWarning($"Invalid request format: {requestString}, using defaults");
            return ("default", 0);
        }

        var topic = parts[0];
        if (!ulong.TryParse(parts[1], out var offset))
        {
            Logger.LogWarning($"Invalid offset in request: {parts[1]}, using 0");
            offset = 0;
        }

        return (topic, offset);
    }
}