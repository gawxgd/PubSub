using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.Segment;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedMessageUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedMessageUseCase>(LogSource.MessageBroker);

    private readonly ICommitLogAppender _commitLogAppender;

    public ProcessReceivedMessageUseCase(ICommitLogFactory commitLogFactory, string topic)
    {
        try
        {
            _commitLogAppender = commitLogFactory.Get(topic);
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

        await _commitLogAppender.AppendAsync(message);
        Logger.LogInfo("Appended message to commit log");
    }
}