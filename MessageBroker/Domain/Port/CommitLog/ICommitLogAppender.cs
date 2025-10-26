namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogAppender
{
    ValueTask AppendAsync(ReadOnlyMemory<byte> payload);
}