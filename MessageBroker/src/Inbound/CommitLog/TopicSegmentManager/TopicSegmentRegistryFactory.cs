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
    
    private readonly ConcurrentDictionary<string, ITopicSegmentRegistry> _managers = new();

    public ITopicSegmentRegistry GetOrCreate(string topic, string directory, ulong baseOffset)
    {
        // Use factory delegate to ensure CreateManager is only called if topic doesn't exist
        return _managers.GetOrAdd(topic, _ => CreateManager(directory, baseOffset));
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

        var activeSegment = segments.Last();
        var currentOffset = RecoverHighWaterMark(activeSegment);
        
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
            entry.Value.Dispose();
        }

        _managers.Clear();
    }
}