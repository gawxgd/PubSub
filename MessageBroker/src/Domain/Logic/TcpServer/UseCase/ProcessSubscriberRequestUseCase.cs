﻿using System.Net.Sockets;
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
            
            var highWaterMark = commitLogReader.GetHighWaterMark();
            Logger.LogDebug($"Requested offset: {offset}, High water mark: {highWaterMark}");
            
            // If requested offset is beyond high water mark, return null
            if (offset > highWaterMark)
            {
                Logger.LogDebug($"Requested offset {offset} is beyond high water mark {highWaterMark}, no batch available");
                return;
            }
            
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
                    $"Read batch with offset {batchOffset} and last offset {lastOffset} (requested: {offset})");

                // Verify that the returned batch actually contains the requested offset
                if (offset < batchOffset || offset > lastOffset)
                {
                    Logger.LogWarning(
                        $"Batch mismatch: requested offset {offset}, but got batch with offset range [{batchOffset}, {lastOffset}]");
                }

                await socket.SendAsync(batchBytes, SocketFlags.None, cancellationToken);

                Logger.LogDebug(
                    $"Send batch with offset {batchOffset} and last offset {lastOffset}");
            }
        }
        catch (FileNotFoundException)
        {
            Logger.LogDebug($"No commit log found for topic '{topic}' - topic may be empty");
            // Send empty response to acknowledge the request, so subscriber knows the request was processed
            // This prevents subscriber from hanging waiting for a response
            try
            {
                // Send a minimal response: 0 bytes (just to acknowledge)
                // Or we could send a special "no data" marker, but for now empty is OK
                // Subscriber will handle empty response gracefully
            }
            catch (Exception ex)
            {
                Logger.LogDebug($"Error sending empty response: {ex.Message}");
            }
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