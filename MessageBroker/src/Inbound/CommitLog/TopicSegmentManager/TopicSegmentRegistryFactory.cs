using System.Collections.Concurrent;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;

namespace MessageBroker.Inbound.CommitLog.TopicSegmentManager;

public sealed class TopicSegmentRegistryFactory(ILogSegmentFactory segmentFactory) : ITopicSegmentRegistryFactory
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<TopicSegmentRegistryFactory>(LogSource.MessageBroker);
    
    private readonly ConcurrentDictionary<string, Lazy<ITopicSegmentRegistry>> _managers = new();

    public ITopicSegmentRegistry GetOrCreate(string topic, string directory, ulong baseOffset)
    {
        var lazy = _managers.GetOrAdd(topic, _ => new Lazy<ITopicSegmentRegistry>(
            () => CreateManager(directory, baseOffset), 
            LazyThreadSafetyMode.ExecutionAndPublication));
        return lazy.Value;
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

            for (var i = 0; i < logFiles.Count; i++)
            {
                var segmentBaseOffset = logFiles[i];
                var segment = segmentFactory.CreateLogSegment(directory, segmentBaseOffset);
                
                if (i < logFiles.Count - 1)
                {
                    var nextSegmentBaseOffset = logFiles[i + 1];
                    segment = segment with { NextOffset = nextSegmentBaseOffset };
                }
                
                segments.Add(segment);
            }
        }

        if (segments.Count == 0)
        {
            var initial = segmentFactory.CreateLogSegment(directory, baseOffset);
            segments.Add(initial);
        }

        var activeSegment = segments.Last();
        var currentOffset = RecoverHighWaterMark(activeSegment);
        
        activeSegment = activeSegment with { NextOffset = currentOffset };
        segments[^1] = activeSegment;
        
        Logger.LogInfo($"Recovered high water mark for segment {activeSegment.LogPath}: {currentOffset}");

        return new TopicSegmentRegistry(activeSegment, currentOffset, segments);
    }

    private ulong RecoverHighWaterMark(LogSegment segment)
    {
        if (!File.Exists(segment.LogPath))
        {
            Logger.LogDebug($"Log file does not exist, using base offset: {segment.BaseOffset}");
            return segment.BaseOffset;
        }

        ILogSegmentReader? reader = null;
        try
        {
            reader = segmentFactory.CreateReader(segment);
            var highWaterMark = reader.RecoverHighWaterMark();
            Logger.LogDebug($"Recovered high water mark from log file: {highWaterMark}");
            return highWaterMark;
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Failed to recover high water mark, using base offset: {ex.Message}");
            return segment.BaseOffset;
        }
        finally
        {
            reader?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    public void Dispose()
    {
        foreach (var entry in _managers)
        {
            if (entry.Value.IsValueCreated)
            {
                entry.Value.Value.Dispose();
            }
        }

        _managers.Clear();
    }
}