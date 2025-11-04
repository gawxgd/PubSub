using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;

public interface ITopicSegmentRegistry : IDisposable
{
    LogSegment GetActiveSegment();
    LogSegment? GetSegmentByOBaseOffset(ulong offset);
    LogSegment? GetSegmentContainingOffset(ulong offset);
    void UpdateActiveSegment(LogSegment newSegment);
    ulong GetHighWaterMark();
    void UpdateCurrentOffset(ulong newOffset);
}