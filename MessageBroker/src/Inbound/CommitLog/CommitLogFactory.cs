using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;

namespace MessageBroker.Inbound.CommitLog;

public sealed class CommitLogFactory(
    ILogSegmentFactory segmentFactory,
    ITopicSegmentRegistryFactory topicSegmentRegistryFactory,
    IOptions<CommitLogOptions> commitLogOptions,
    IOptions<List<CommitLogTopicOptions>> commitLogTopicOptions)
    : ICommitLogFactory, IAsyncDisposable
{
    private readonly CommitLogOptions _commitLogOptions = commitLogOptions.Value;
    private readonly List<CommitLogTopicOptions> _commitLogTopicOptions = commitLogTopicOptions.Value;
    private readonly ConcurrentDictionary<string, ICommitLogAppender> _appenders = new();
    private readonly ConcurrentDictionary<string, ICommitLogReader> _readers = new();

    public ICommitLogAppender GetAppender(string topic)
    {
        return _appenders.GetOrAdd(topic, CreateAppender);
    }

    public ICommitLogReader GetReader(string topic)
    {
        return _readers.GetOrAdd(topic, CreateReader);
    }

    private ICommitLogAppender CreateAppender(string topic)
    {
        var topicOpt = _commitLogTopicOptions
            .FirstOrDefault(t => string.Equals(t.Name, topic, StringComparison.OrdinalIgnoreCase));

        if (topicOpt == null)
        {
            throw new InvalidOperationException($"Topic '{topic}' is not configured.");
        }

        var directory = topicOpt.Directory ?? Path.Join(_commitLogOptions.Directory, topic);
        var baseOffset = topicOpt.BaseOffset;
        var flushInterval = TimeSpan.FromMilliseconds(topicOpt.FlushIntervalMs);
        var manager = topicSegmentRegistryFactory.GetOrCreate(topic, directory, baseOffset);

        // Use the recovered high water mark from the manager, not the config baseOffset
        return new BinaryCommitLogAppender(segmentFactory, directory, manager.GetHighWaterMark(), flushInterval, manager);
    }

    private ICommitLogReader CreateReader(string topic)
    {
        var topicOpt = _commitLogTopicOptions
            .FirstOrDefault(t => string.Equals(t.Name, topic, StringComparison.OrdinalIgnoreCase));

        if (topicOpt == null)
        {
            throw new InvalidOperationException($"Topic '{topic}' is not configured.");
        }

        var directory = topicOpt.Directory ?? Path.Join(_commitLogOptions.Directory, topic);
        var baseOffset = topicOpt.BaseOffset;
        var manager = topicSegmentRegistryFactory.GetOrCreate(topic, directory, baseOffset);

        return new BinaryCommitLogReader(segmentFactory, manager);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var entry in _appenders)
        {
            await entry.Value.DisposeAsync();
        }

        _appenders.Clear();

        foreach (var entry in _readers)
        {
            await entry.Value.DisposeAsync();
        }

        _readers.Clear();
    }
}