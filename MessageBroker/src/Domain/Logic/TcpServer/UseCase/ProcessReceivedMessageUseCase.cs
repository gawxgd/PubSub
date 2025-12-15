using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedMessageUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedMessageUseCase>(LogSource.MessageBroker);

    private readonly ICommitLogAppender _commitLogAppender;
    private readonly ILogRecordBatchReader _batchReader;

    public ProcessReceivedMessageUseCase(
        ICommitLogFactory commitLogFactory,
        ILogRecordBatchReader batchReader,
        string topic)
    {
        _batchReader = batchReader;

        try
        {
            _commitLogAppender = commitLogFactory.GetAppender(topic);
        }
        catch (Exception ex)
        {
            Logger.LogError("Failed to get commit log appender", ex);
            throw;
        }
    }

    public async Task ProcessMessageAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        Logger.LogDebug($"Processing message {message.Length} bytes");

        try
        {
            using var stream = new MemoryStream(message.ToArray());
            var batch = _batchReader.ReadBatch(stream);

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
