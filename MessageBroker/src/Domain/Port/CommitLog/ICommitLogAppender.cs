namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogAppender : IAsyncDisposable
{
    ValueTask<ulong> AppendAsync(ReadOnlyMemory<byte> payload);
}