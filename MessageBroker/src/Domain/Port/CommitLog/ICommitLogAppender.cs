namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogAppender : IAsyncDisposable
{
    ValueTask AppendAsync(ReadOnlyMemory<byte> payload);
}