using System.Buffers.Binary;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Exceptions;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog.Segment;

public sealed class BinaryLogSegmentReader : ILogSegmentReader
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<BinaryLogSegmentReader>(LogSource.MessageBroker);

    private readonly LogSegment _segment;
    private readonly ILogRecordBatchReader _batchReader;
    private readonly FileStream _log;
    private readonly FileStream? _index;
    private readonly FileStream? _timeIndex;
    private readonly IOffsetIndexReader _offsetIndexReader;
    private readonly ITimeIndexReader _timeIndexReader;
    private readonly SemaphoreSlim _readLock = new(1, 1);
    private bool _disposed;

    public BinaryLogSegmentReader(
        ILogRecordBatchReader batchReader,
        LogSegment segment,
        IOffsetIndexReader offsetIndexReader,
        ITimeIndexReader timeIndexReader,
        uint logBufferSize,
        uint indexBufferSize)
    {
        _segment = segment;
        _batchReader = batchReader;
        _offsetIndexReader = offsetIndexReader;
        _timeIndexReader = timeIndexReader;

        if (!File.Exists(segment.LogPath))
        {
            throw new FileNotFoundException($"Log segment file not found: {segment.LogPath}");
        }

        _log = new FileStream(
            segment.LogPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            bufferSize: (int)logBufferSize,
            FileOptions.SequentialScan);

        if (File.Exists(segment.IndexFilePath))
        {
            _index = new FileStream(
                segment.IndexFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                bufferSize: (int)indexBufferSize,
                FileOptions.RandomAccess);
        }

        if (File.Exists(segment.TimeIndexFilePath))
        {
            _timeIndex = new FileStream(
                segment.TimeIndexFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                bufferSize: (int)indexBufferSize,
                FileOptions.RandomAccess);
        }
    }

    public (byte[] batchBytes, ulong baseOffset, ulong lastOffset)? ReadBatchBytes(ulong offset)
    {
        Logger.LogDebug($"ReadBatchBytes called with offset={offset}, segment baseOffset={_segment.BaseOffset}");
        
        return ReadBatchCore(
            offset,
            log =>
            {
                var (batchBytes, batchOffset, lastOffset) =
                    _batchReader.ReadBatchBytesAndAdvance(log);

                Logger.LogDebug($"ReadBatchBytes: read batch at position, batchOffset={batchOffset}, lastOffset={lastOffset}, requestedOffset={offset}");

                if (offset >= batchOffset && offset <= lastOffset)
                {
                    Logger.LogDebug($"ReadBatchBytes: FOUND - offset {offset} is within [{batchOffset}, {lastOffset}]");
                    return (true, false, (batchBytes, batchOffset, lastOffset));
                }

                if (batchOffset > offset)
                {
                    Logger.LogDebug($"ReadBatchBytes: PASSED - batchOffset {batchOffset} > requestedOffset {offset}");
                    return (false, true, default);
                }

                Logger.LogDebug($"ReadBatchBytes: CONTINUE - batchOffset {batchOffset} <= requestedOffset {offset} but lastOffset {lastOffset} < requestedOffset");
                return (false, false, default);
            });
    }

    public LogRecordBatch? ReadBatch(ulong offset)
    {
        return ReadBatchCore(
            offset,
            log =>
            {
                var batch = _batchReader.ReadBatch(log);

                if (offset >= batch.BaseOffset && offset <= batch.LastOffset)
                    return (true, false, batch);

                if (batch.BaseOffset > offset)
                    return (false, true, default);

                return (false, false, default);
            });
    }

    private T? ReadBatchCore<T>(
        ulong offset,
        Func<Stream, (bool found, bool passed, T value)> readNext)
    {
        if (offset < _segment.BaseOffset)
        {
            throw new SegmentReaderException(
                "Start read offset is smaller than segment base offset, trying to read from wrong file");
        }
        
        _readLock.Wait();
        try
        {
            var fileInfo = new FileInfo(_segment.LogPath);
            fileInfo.Refresh();
            var currentLength = fileInfo.Length;
            
            var position = FindPositionForOffset(offset);
            Logger.LogDebug($"ReadBatchCore: seeking to position {position} for offset {offset}, file length={currentLength}");
            _log.Seek((long)position, SeekOrigin.Begin);

            var iterations = 0;
            while (_log.Position < currentLength)
            {
                iterations++;
                Logger.LogDebug($"ReadBatchCore: iteration {iterations}, position={_log.Position}, length={_log.Length}");
                try
                {
                    var (found, passed, value) = readNext(_log);

                    if (found)
                    {
                        Logger.LogDebug($"ReadBatchCore: returning FOUND value after {iterations} iterations");
                        return value;
                    }

                    if (passed)
                    {
                        Logger.LogDebug($"ReadBatchCore: PASSED after {iterations} iterations");
                        break;
                    }
                }
                catch (EndOfStreamException ex)
                {
                    Logger.LogDebug($"ReadBatchCore: EndOfStreamException after {iterations} iterations: {ex.Message}");
                    break;
                }
            }

            Logger.LogDebug($"ReadBatchCore: returning NULL after {iterations} iterations, final position={_log.Position}, length={_log.Length}");
            return default;
        }
        finally
        {
            _readLock.Release();
        }
    }

    public IEnumerable<LogRecordBatch> ReadRange(ulong startOffset, ulong endOffset)
    {
        if (startOffset >= endOffset)
        {
            yield break;
        }

        _readLock.Wait();
        try
        {
            var position = FindPositionForOffset(startOffset);
            _log.Seek((long)position, SeekOrigin.Begin);

            while (_log.Position < _log.Length)
            {
                LogRecordBatch batch;
                try
                {
                    batch = _batchReader.ReadBatch(_log);
                }
                catch (EndOfStreamException)
                {
                    yield break;
                }

                if (batch.LastOffset <= startOffset)
                {
                    continue;
                }

                if (batch.BaseOffset >= endOffset)
                {
                    yield break;
                }

                yield return batch;
            }
        }
        finally
        {
            _readLock.Release();
        }
    }

    public IEnumerable<LogRecordBatch> ReadFromTimestamp(ulong timestamp)
    {
        _readLock.Wait();
        try
        {
            var position = FindPositionForTimestamp(timestamp);
            _log.Seek((long)position, SeekOrigin.Begin);

            while (_log.Position < _log.Length)
            {
                LogRecordBatch batch;
                try
                {
                    batch = _batchReader.ReadBatch(_log);
                }
                catch (EndOfStreamException)
                {
                    yield break;
                }

                if (batch.BaseTimestamp >= timestamp)
                {
                    yield return batch;
                }
            }
        }
        finally
        {
            _readLock.Release();
        }
    }

    private ulong FindPositionForOffset(ulong offset)
    {
        if (_index == null || _index.Length == 0)
            return 0;

        ulong relative = offset - _segment.BaseOffset;
        int entryCount = (int)(_index.Length / OffsetIndexEntry.Size);

        var bestEntry = IndexBinarySearch.Search(
            entryCount,
            readEntryAt: ReadIndexEntry,
            getKey: e => e.RelativeOffset,
            targetKey: relative
        );

        if (bestEntry == null)
        {
            return 0;
        }

        return bestEntry.FilePosition;
    }

    private ulong FindPositionForTimestamp(ulong timestamp)
    {
        if (_timeIndex == null || _timeIndex.Length == 0)
            return 0;

        int entryCount = (int)(_timeIndex.Length / TimeIndexEntry.Size);

        var bestEntry = IndexBinarySearch.Search(
            entryCount,
            readEntryAt: ReadTimeIndexEntry,
            getKey: e => e.Timestamp,
            targetKey: timestamp
        );

        if (bestEntry == null)
        {
            return 0;
        }

        return bestEntry.FilePosition;
    }

    private OffsetIndexEntry ReadIndexEntry(int entryIndex)
    {
        _index!.Seek(entryIndex * OffsetIndexEntry.Size, SeekOrigin.Begin);
        return _offsetIndexReader.ReadFrom(_index);
    }

    private TimeIndexEntry ReadTimeIndexEntry(int entryIndex)
    {
        _timeIndex!.Seek(entryIndex * TimeIndexEntry.Size, SeekOrigin.Begin);
        return _timeIndexReader.ReadFrom(_timeIndex);
    }

    public ulong RecoverHighWaterMark()
    {
        _readLock.Wait();
        try
        {
            if (_log.Length == 0)
            {
                return _segment.BaseOffset;
            }

            _log.Seek(0, SeekOrigin.Begin);
            ulong highWaterMark = _segment.BaseOffset;

            while (_log.Position < _log.Length)
            {
                try
                {
                    var (_, _, lastOffset) = _batchReader.ReadBatchBytesAndAdvance(_log);
                    highWaterMark = lastOffset + 1;
                }
                catch (EndOfStreamException)
                {
                    break;
                }
                catch (InvalidDataException)
                {
                    break;
                }
            }

            return highWaterMark;
        }
        finally
        {
            _readLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _log.DisposeAsync();

        if (_index != null)
        {
            await _index.DisposeAsync();
        }

        if (_timeIndex != null)
        {
            await _timeIndex.DisposeAsync();
        }

        _readLock.Dispose();
        _disposed = true;
    }
}
