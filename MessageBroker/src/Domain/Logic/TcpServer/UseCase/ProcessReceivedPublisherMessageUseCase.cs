using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedPublisherMessageUseCase(
    ICommitLogFactory commitLogFactory,
    string topic,
    ILogRecordBatchReader batchReader) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedPublisherMessageUseCase>(LogSource.MessageBroker);

    private readonly ICommitLogAppender _commitLogAppender = commitLogFactory.GetAppender(topic);

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        Logger.LogDebug($"Processing message {message.Length} bytes");

        try
        {
            using var stream = new MemoryStream(message.ToArray());
            var batch = batchReader.ReadBatch(stream);

            Logger.LogDebug($"Parsed batch with {batch.Records.Count} records");

            foreach (var record in batch.Records)
            {
                await _commitLogAppender.AppendAsync(record.Payload);
            }

            Logger.LogInfo($"Appended {batch.Records.Count} records to commit log");
        }
        catch (InvalidDataException ex)
        {
            Logger.LogError($"Failed to parse batch: {ex.Message}", ex);
            throw;
        }
    }
}