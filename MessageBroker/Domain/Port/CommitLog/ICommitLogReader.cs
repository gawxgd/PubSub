using System.Threading.Channels;

namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogReader
{
    Task ReadAsync(Channel<byte[]> channel);
}