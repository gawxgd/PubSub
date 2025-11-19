using System.Threading.Channels;
using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Segment;

public interface ILogSegmentReader
{
    void ReadAll(Channel<byte[]> channel);
    void ReadFromOffset(ulong currentOffset, Channel<byte[]> channel);
    LogSegment GetSegment();
}