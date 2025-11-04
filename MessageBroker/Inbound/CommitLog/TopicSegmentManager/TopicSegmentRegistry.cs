using System.Collections.Concurrent;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;

namespace MessageBroker.Inbound.CommitLog.TopicSegmentManager;

public sealed class TopicSegmentRegistry(
    LogSegment activeSegment,
    ulong currentOffset,
    IEnumerable<LogSegment>? existingSegments = null) : ITopicSegmentRegistry
{
    private LogSegment _activeSegment = activeSegment;
    private ulong _currentOffset = currentOffset;
    private readonly ReaderWriterLockSlim _lock = new();

    private readonly SortedList<ulong, LogSegment> _allSegments = new(
        (existingSegments ?? [activeSegment])
        .ToDictionary(segment => segment.BaseOffset, segment => segment)
    );

    public LogSegment GetActiveSegment()
    {
        _lock.EnterReadLock();
        try
        {
            return _activeSegment;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public LogSegment? GetSegmentByOBaseOffset(ulong offset)
    {
        return _allSegments.GetValueOrDefault(offset);
    }

    public LogSegment? GetSegmentContainingOffset(ulong offset)
    {
        //ToDo rewrite this
        _lock.EnterReadLock();
        try
        {
            if (offset >= _activeSegment.BaseOffset && offset < _activeSegment.NextOffset)
            {
                return _activeSegment;
            }

            if (_allSegments.Count == 0)
            {
                return null;
            }

            var keys = _allSegments.Keys;
            var low = 0;
            var high = keys.Count - 1;
            var candidateIndex = -1;

            while (low <= high)
            {
                var mid = low + ((high - low) >> 1);
                var baseOffset = keys[mid];

                if (baseOffset == offset)
                {
                    candidateIndex = mid;
                    break;
                }

                if (baseOffset < offset)
                {
                    candidateIndex = mid;
                    low = mid + 1;
                }
                else
                {
                    high = mid - 1;
                }
            }

            if (candidateIndex < 0)
                return null;

            var candidate = _allSegments.Values[candidateIndex];

            ulong nextBaseOffset = candidateIndex + 1 < keys.Count
                ? keys[candidateIndex + 1]
                : ulong.MaxValue;

            return (offset >= candidate.BaseOffset && offset < nextBaseOffset)
                ? candidate
                : null;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }


    public void UpdateActiveSegment(LogSegment newSegment)
    {
        _lock.EnterWriteLock();
        try
        {
            _activeSegment = newSegment;
            _allSegments.TryAdd(newSegment.BaseOffset, newSegment);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public ulong GetHighWaterMark()
    {
        _lock.EnterReadLock();
        try
        {
            return _currentOffset;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void UpdateCurrentOffset(ulong newOffset)
    {
        _lock.EnterWriteLock();
        try
        {
            _currentOffset = newOffset;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        _lock.Dispose();
    }
}