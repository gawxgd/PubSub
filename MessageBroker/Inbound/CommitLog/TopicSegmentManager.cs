using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Inbound.CommitLog;

public sealed class TopicSegmentManager(LogSegment initialSegment, ulong currentOffset) : ITopicSegmentManager
{
    private LogSegment _activeSegment = initialSegment;
    private ulong _currentOffset = currentOffset;
    private readonly ReaderWriterLockSlim _lock = new();

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

    public void UpdateActiveSegment(LogSegment newSegment)
    {
        _lock.EnterWriteLock();
        try
        {
            _activeSegment = newSegment;
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