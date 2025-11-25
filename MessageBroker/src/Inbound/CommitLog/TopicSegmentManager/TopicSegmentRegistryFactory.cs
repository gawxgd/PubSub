using System.Collections.Concurrent;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;

namespace MessageBroker.Inbound.CommitLog.TopicSegmentManager;

public sealed class TopicSegmentRegistryFactory(ILogSegmentFactory segmentFactory) : ITopicSegmentRegistryFactory
{
    private readonly ConcurrentDictionary<string, ITopicSegmentRegistry> _managers = new();

    public ITopicSegmentRegistry GetOrCreate(string topic, string directory, ulong baseOffset)
    {
        return _managers.GetOrAdd(topic, CreateManager(directory, baseOffset));
    }

    private ITopicSegmentRegistry CreateManager(string directory, ulong baseOffset)
    {
        var segments = new List<LogSegment>();

        if (Directory.Exists(directory))
        {
            var logFiles = Directory.GetFiles(directory, "*.log")
                .Select(Path.GetFileNameWithoutExtension)
                .Where(file => ulong.TryParse(file, out _))
                .Select(file => ulong.Parse(file!))
                .OrderBy(offset => offset)
                .ToList();

            foreach (var offset in logFiles)
            {
                var segment = segmentFactory.CreateLogSegment(directory, offset);
                segments.Add(segment);
            }
        }

        if (segments.Count == 0)
        {
            var initial = segmentFactory.CreateLogSegment(directory, baseOffset);
            segments.Add(initial);
        }

        var activeSegment = segments.Last(); // Most recent segment
        var currentOffset = activeSegment.NextOffset; // Or determine from segment file

        return new TopicSegmentRegistry(activeSegment, currentOffset, segments);
    }

    public void Dispose()
    {
        foreach (var entry in _managers)
        {
            entry.Value.Dispose();
        }

        _managers.Clear();
    }
}