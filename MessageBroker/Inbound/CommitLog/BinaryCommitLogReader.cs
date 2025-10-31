using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog;

public sealed class BinaryCommitLogReader : ICommitLogReader
{
    // ToDo batch reading instead of one record reading
    private readonly string _directory;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly List<LogSegment> _segments;
    private readonly Dictionary<string, ILogSegmentReader> _readerCache;
    private readonly object _lock = new();
    private bool _disposed;

    public BinaryCommitLogReader(
        ILogSegmentFactory segmentFactory,
        string directory)
    {
        _segmentFactory = segmentFactory;
        _directory = directory;
        _segments = LoadSegments();
        _readerCache = new Dictionary<string, ILogSegmentReader>();
    }

    public LogRecord? ReadRecord(ulong offset)
    {
        ThrowIfDisposed();

        var segment = FindSegmentForOffset(offset);
        if (segment == null) return null;

        var reader = GetOrCreateReader(segment);
        var batch = reader.ReadBatch(offset);

        return batch?.Records.FirstOrDefault(r => r.Offset == offset);
    }

    public IEnumerable<LogRecord> ReadRecords(ulong startOffset, int maxRecords)
    {
        ThrowIfDisposed();

        if (maxRecords <= 0)
        {
            yield break;
        }

        var count = 0;

        var relevantSegments = _segments
            .Where(s => s.NextOffset > startOffset || s.NextOffset == s.BaseOffset)
            .OrderBy(s => s.BaseOffset);

        foreach (var segment in relevantSegments)
        {
            if (count >= maxRecords) yield break;

            if (segment.NextOffset > 0 && segment.NextOffset <= startOffset)
            {
                continue;
            }

            var reader = GetOrCreateReader(segment);

            foreach (var batch in reader.ReadRange(startOffset, ulong.MaxValue))
            {
                foreach (var record in batch.Records)
                {
                    if (record.Offset >= startOffset)
                    {
                        yield return record;
                        count++;

                        if (count >= maxRecords) yield break;
                    }
                }
            }
        }
    }

    public IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp, int maxRecords)
    {
        ThrowIfDisposed();

        if (maxRecords <= 0)
        {
            yield break;
        }

        var count = 0;

        foreach (var segment in _segments.OrderBy(s => s.BaseOffset))
        {
            if (count >= maxRecords) yield break;

            var reader = GetOrCreateReader(segment);

            foreach (var batch in reader.ReadFromTimestamp(timestamp))
            {
                foreach (var record in batch.Records)
                {
                    if (record.Timestamp >= timestamp)
                    {
                        yield return record;
                        count++;

                        if (count >= maxRecords) yield break;
                    }
                }
            }
        }
    }

    public ulong GetHighWaterMark()
    {
        ThrowIfDisposed();

        if (_segments.Count == 0)
        {
            return 0;
        }

        var lastSegment = _segments.OrderByDescending(s => s.BaseOffset).First();

        return lastSegment.NextOffset > lastSegment.BaseOffset
            ? lastSegment.NextOffset - 1
            : 0;
    }

    public void RefreshSegments()
    {
        ThrowIfDisposed();

        lock (_lock)
        {
            // Load new segments
            var newSegments = LoadSegments();

            // Find segments that no longer exist and dispose their readers
            var removedSegments = _segments
                .Where(s => !newSegments.Any(ns => ns.LogPath == s.LogPath))
                .ToList();

            foreach (var removed in removedSegments)
            {
                if (_readerCache.TryGetValue(removed.LogPath, out var reader))
                {
                    reader.Dispose();
                    _readerCache.Remove(removed.LogPath);
                }
            }

            _segments.Clear();
            _segments.AddRange(newSegments);
        }
    }

    private ILogSegmentReader GetOrCreateReader(LogSegment segment)
    {
        lock (_lock)
        {
            if (_readerCache.TryGetValue(segment.LogPath, out var existingReader))
            {
                return existingReader;
            }

            var reader = _segmentFactory.CreateReader(segment);
            _readerCache[segment.LogPath] = reader;
            return reader;
        }
    }

    private List<LogSegment> LoadSegments()
    {
        if (!Directory.Exists(_directory))
        {
            return new List<LogSegment>();
        }

        var logFiles = Directory.GetFiles(_directory, "*.log", SearchOption.TopDirectoryOnly);
        var segments = new List<LogSegment>();

        foreach (var logFile in logFiles)
        {
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(logFile);

                if (ulong.TryParse(fileName, out var baseOffset))
                {
                    var indexPath = Path.ChangeExtension(logFile, ".index");
                    var timeIndexPath = Path.ChangeExtension(logFile, ".timeindex");

                    var nextOffset = DetermineNextOffset(logFile, baseOffset);

                    var segment = new LogSegment(
                        logFile,
                        indexPath,
                        timeIndexPath,
                        baseOffset,
                        nextOffset);

                    segments.Add(segment);
                }
            }
            catch
            {
                // Skip corrupted or invalid segment files
            }
        }

        return segments.OrderBy(s => s.BaseOffset).ToList();
    }

    private ulong DetermineNextOffset(string logPath, ulong baseOffset)
    {
        if (!File.Exists(logPath))
        {
            return baseOffset;
        }

        var fileInfo = new FileInfo(logPath);
        if (fileInfo.Length == 0)
        {
            return baseOffset;
        }

        try
        {
            var tempSegment = new LogSegment(
                logPath,
                Path.ChangeExtension(logPath, ".index"),
                Path.ChangeExtension(logPath, ".timeindex"),
                baseOffset,
                baseOffset);

            // Use cached reader if available for efficiency
            ILogSegmentReader? reader = null;
            var useCachedReader = false;

            lock (_lock)
            {
                if (_readerCache.TryGetValue(logPath, out reader))
                {
                    useCachedReader = true;
                }
            }

            if (!useCachedReader)
            {
                reader = _segmentFactory.CreateReader(tempSegment);
            }

            try
            {
                LogRecordBatch? lastBatch = null;
                foreach (var batch in reader.ReadRange(baseOffset, ulong.MaxValue))
                {
                    lastBatch = batch;
                }

                if (lastBatch != null)
                {
                    return lastBatch.BaseOffset + (ulong)lastBatch.Records.Count;
                }

                return baseOffset;
            }
            finally
            {
                if (!useCachedReader)
                {
                    reader?.Dispose();
                }
            }
        }
        catch
        {
            return baseOffset;
        }
    }

    private LogSegment? FindSegmentForOffset(ulong offset)
    {
        if (_segments.Count == 0)
        {
            return null;
        }

        int left = 0;
        int right = _segments.Count - 1;

        while (left <= right)
        {
            int mid = left + (right - left) / 2;
            var segment = _segments[mid];

            if (offset >= segment.BaseOffset && offset < segment.NextOffset)
            {
                return segment;
            }
            else if (offset >= segment.NextOffset)
            {
                left = mid + 1;
            }
            else
            {
                right = mid - 1;
            }
        }

        var lastSegment = _segments[^1];
        if (offset >= lastSegment.BaseOffset)
        {
            return lastSegment;
        }

        return null;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(BinaryCommitLogReader));
        }
    }

    public void Dispose()
    {
        if (_disposed) return;

        lock (_lock)
        {
            foreach (var reader in _readerCache.Values)
            {
                reader.Dispose();
            }

            _readerCache.Clear();
            _segments.Clear();
            _disposed = true;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        foreach (var reader in _readerCache.Values)
        {
            await reader.DisposeAsync();
        }

        _readerCache.Clear();
        _segments.Clear();
        _disposed = true;
    }
}