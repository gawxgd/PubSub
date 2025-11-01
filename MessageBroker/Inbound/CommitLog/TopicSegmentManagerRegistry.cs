using System.Collections.Concurrent;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog;

public sealed class TopicSegmentManagerRegistry(ILogSegmentFactory segmentFactory) : ITopicSegmentManagerRegistry
{
    private readonly ConcurrentDictionary<string, ITopicSegmentManager> _managers = new();

    public ITopicSegmentManager GetOrCreate(string topic, string directory, ulong baseOffset)
    {
        return _managers.GetOrAdd(topic, CreateManager(directory, baseOffset));
    }

    private ITopicSegmentManager CreateManager(string directory, ulong baseOffset)
    {
        var initial = segmentFactory.CreateLogSegment(directory, baseOffset);
        return new TopicSegmentManager(initial, baseOffset);
    }

    public void Dispose()
    {
        foreach (var kv in _managers)
            kv.Value.Dispose();
        _managers.Clear();
    }
}


