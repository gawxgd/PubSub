using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Segment;

public interface ILogSegmentFactory
{
    ILogSegmentWriter CreateWriter(LogSegment segment);
    ILogSegmentReader CreateReader(LogSegment segment);
    LogSegment CreateLogSegment(string directory, ulong baseOffset);
}