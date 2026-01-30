using MessageBroker.Inbound.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;

public interface ITopicSegmentRegistryFactory : IDisposable
{
    ITopicSegmentRegistry GetOrCreate(string topic, string directory, ulong baseOffset);
}
