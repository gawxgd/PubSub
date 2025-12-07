﻿using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessSubscriberRequestUseCase(Socket socket, ICommitLogFactory commitLogFactory)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessSubscriberRequestUseCase>(LogSource.MessageBroker);
    
    public async Task ProcessRequestAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        // TODO read topic and offset from message
        var topic = "default";
        ulong offset = 0;
        
        Logger.LogDebug($"Processing subscriber request: {message.Length} bytes");
        var commitLogReader = commitLogFactory.GetReader(topic);
        
        try
        {
            ulong currentOffset = offset;
            
            while (!cancellationToken.IsCancellationRequested)
            {
                var batch = commitLogReader.ReadRecordBatch(currentOffset);
                
                if (batch == null)
                {
                    Logger.LogDebug($"No more batches available at offset {currentOffset}");
                    break;
                }
                
                Logger.LogDebug($"Read batch with {batch.Records.Count} records, base offset: {batch.BaseOffset}, last offset: {batch.LastOffset}");
                
                foreach (var record in batch.Records)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    
                    var payload = record.Payload;
                    if (payload.Length > 0)
                    {
                        await socket.SendAsync(payload, SocketFlags.None, cancellationToken);
                        Logger.LogDebug($"Sent record at offset {record.Offset}: {payload.Length} bytes");
                    }
                }
                
                currentOffset = batch.LastOffset + 1;
                
                var highWaterMark = commitLogReader.GetHighWaterMark();
                if (currentOffset > highWaterMark)
                {
                    Logger.LogDebug($"Reached high water mark: {highWaterMark}");
                    break;
                }
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
            await commitLogReader.DisposeAsync();
        }
        
        Logger.LogInfo("Messages from commit log sent to subscriber");
    }
}