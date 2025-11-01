using System.Collections.Concurrent;
using MessageBroker.Domain.Entities.CommitLog;
using Microsoft.Extensions.Options;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;

namespace MessageBroker.Inbound.CommitLog;

public sealed class CommitLogFactory(
    ILogSegmentFactory segmentFactory,
    IOptions<CommitLogOptions> commitLogOptions,
    IOptions<List<CommitLogTopicOptions>> commitLogTopicOptions)
    : ICommitLogFactory, IDisposable
{
    private readonly CommitLogOptions _commitLogOptions = commitLogOptions.Value;
    private readonly List<CommitLogTopicOptions> _commitLogTopicOptions = commitLogTopicOptions.Value;
    private readonly ConcurrentDictionary<string, ICommitLogAppender> _appenders = new();
    private readonly ConcurrentDictionary<string, TopicSegmentManager> _segmentManagers = new();

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

        return new BinaryCommitLogAppender(segmentFactory, directory, baseOffset, flushInterval, topic);
    }

    private ICommitLogReader CreateReader(string topic)
    {
        var directory = Path.Combine(_commitLogOptions.Directory, topic);

        return new BinaryCommitLogReader
    }

    public void Dispose()
    {
        //ToDo fix
        foreach (var kv in _appenders)
            (kv.Value as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _appenders.Clear();
    }
}