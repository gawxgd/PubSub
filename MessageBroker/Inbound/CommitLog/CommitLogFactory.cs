using System.Collections.Concurrent;
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

    public ICommitLogAppender GetAppender(string topic)
    {
        return _appenders.GetOrAdd(topic, CreateAppender);
    }

    public ICommitLogReader GetReader(string topic, ulong offset)
    {
        //TODO
        return new BinaryCommitLogReader(segmentFactory, topic, offset);
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


    public void Dispose()
    {
        //ToDo fix
        foreach (var kv in _appenders)
            (kv.Value as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _appenders.Clear();
    }
}