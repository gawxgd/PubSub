using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;

namespace MessageBroker.Inbound.CommitLog;

public sealed class CommitLogFactory(
    ILogSegmentFactory segmentFactory,
    ITopicSegmentManagerRegistry segmentRegistry,
    IOptions<CommitLogOptions> commitLogOptions,
    IOptions<List<CommitLogTopicOptions>> commitLogTopicOptions)
    : ICommitLogFactory, IDisposable
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

        var directory = topicOpt.Directory ?? Path.Combine(_commitLogOptions.Directory, topic);
        var baseOffset = topicOpt.BaseOffset;
        var flushInterval = TimeSpan.FromMilliseconds(topicOpt.FlushIntervalMs);
        var manager = segmentRegistry.GetOrCreate(topic, directory, baseOffset);
        
        return new BinaryCommitLogAppender(segmentFactory, directory, baseOffset, flushInterval, topic, manager);
    }

    private ICommitLogReader CreateReader(string topic)
    {
        var topicOpt = _commitLogTopicOptions
            .FirstOrDefault(t => string.Equals(t.Name, topic, StringComparison.OrdinalIgnoreCase));

        if (topicOpt == null)
        {
            throw new InvalidOperationException($"Topic '{topic}' is not configured.");
        }

        var directory = topicOpt.Directory ?? Path.Combine(_commitLogOptions.Directory, topic);
        var baseOffset = topicOpt.BaseOffset;
        var manager = segmentRegistry.GetOrCreate(topic, directory, baseOffset);
        
        return new BinaryCommitLogReader(segmentFactory, manager, topic);
    }

    public void Dispose()
    {
        foreach (var kv in _appenders)
            (kv.Value as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _appenders.Clear();
        foreach (var kv in _readers)
            (kv.Value as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _readers.Clear();
    }
}