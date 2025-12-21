using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Exceptions;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog.Segment;

public sealed class BinaryLogSegmentReader : ILogSegmentReader
{
    private readonly LogSegment _segment;
    private readonly ILogRecordBatchReader _batchReader;
    private readonly FileStream _log;
    private readonly FileStream? _index;
    private readonly FileStream? _timeIndex;
    private readonly IOffsetIndexReader _offsetIndexReader;
    private readonly ITimeIndexReader _timeIndexReader;
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
            FileShare.ReadWrite, // Allow concurrent writes
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
        return ReadBatchCore(
            offset,
            log =>
            {
                var (batchBytes, batchOffset, lastOffset) =
                    _batchReader.ReadBatchBytesAndAdvance(log);

                if (offset >= batchOffset && offset <= lastOffset)
                    return (true, false, (batchBytes, batchOffset, lastOffset));

                if (batchOffset > offset)
                    return (false, true, default);

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

        var position = FindPositionForOffset(offset);
        _log.Seek((long)position, SeekOrigin.Begin);

        while (_log.Position < _log.Length)
        {
            try
            {
                var (found, passed, value) = readNext(_log);

                if (found)
                {
                    return value;
                }

                if (passed)
                {
                    break;
                }
            }
            catch (EndOfStreamException)
            {
                break;
            }
        }

        return default;
    }

    public IEnumerable<LogRecordBatch> ReadRange(ulong startOffset, ulong endOffset)
    {
        // ToDo will it be needed
        if (startOffset >= endOffset)
        {
            yield break;
        }

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


            // If batch is completely before our range, skip
            if (batch.LastOffset <= startOffset)
            {
                continue;
            }

            // If batch starts at or after end of range, stop
            if (batch.BaseOffset >= endOffset)
            {
                yield break;
            }

            // Batch overlaps with our range
            yield return batch;
        }
    }

    public IEnumerable<LogRecordBatch> ReadFromTimestamp(ulong timestamp)
    {
        //ToDo fix timestamp handling right now it saves offset not position in file
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

            // Only return batches at or after the timestamp
            if (batch.BaseTimestamp >= timestamp)
            {
                yield return batch;
            }
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

        _disposed = true;
    }
}