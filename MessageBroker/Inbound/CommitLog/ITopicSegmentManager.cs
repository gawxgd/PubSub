using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Inbound.CommitLog;

public interface ITopicSegmentManager : IDisposable
{
    LogSegment GetActiveSegment();
    void UpdateActiveSegment(LogSegment newSegment);
    ulong GetHighWaterMark();
    void UpdateCurrentOffset(ulong newOffset);
}


