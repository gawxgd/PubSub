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
using Microsoft.Win32.SafeHandles;

namespace MessageBroker.Inbound.CommitLog.Segment;

public sealed class BinaryLogSegmentReader : ILogSegmentReader
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<BinaryLogSegmentReader>(LogSource.MessageBroker);

    private const int BatchHeaderSize = 24;

    private readonly LogSegment _segment;
    private readonly ILogRecordBatchReader _batchReader;
    private readonly SafeFileHandle _logHandle;
    private readonly SafeFileHandle? _indexHandle;
    private readonly SafeFileHandle? _timeIndexHandle;
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

        _logHandle = File.OpenHandle(
            segment.LogPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            FileOptions.Asynchronous);

        if (File.Exists(segment.IndexFilePath))
        {
            _indexHandle = File.OpenHandle(
                segment.IndexFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                FileOptions.Asynchronous);
        }

        if (File.Exists(segment.TimeIndexFilePath))
        {
            _timeIndexHandle = File.OpenHandle(
                segment.TimeIndexFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                FileOptions.Asynchronous);
        }
    }

    public (byte[] batchBytes, ulong baseOffset, ulong lastOffset)? ReadBatchBytes(ulong offset)
    {
        Logger.LogDebug($"ReadBatchBytes called with offset={offset}, segment baseOffset={_segment.BaseOffset}");
        
        return ReadBatchCore(
            offset,
            (data) =>
            {
                var (batchBytes, batchOffset, lastOffset, bytesConsumed) =
                    _batchReader.ReadBatchBytesAndAdvance(data);

                Logger.LogDebug($"ReadBatchBytes: read batch at position, batchOffset={batchOffset}, lastOffset={lastOffset}, requestedOffset={offset}");

                if (offset >= batchOffset && offset <= lastOffset)
                {
                    Logger.LogDebug($"ReadBatchBytes: FOUND - offset {offset} is within [{batchOffset}, {lastOffset}]");
                    return (true, false, (batchBytes, batchOffset, lastOffset), bytesConsumed);
                }

                if (batchOffset > offset)
                {
                    Logger.LogDebug($"ReadBatchBytes: PASSED - batchOffset {batchOffset} > requestedOffset {offset}");
                    return (false, true, default, bytesConsumed);
                }

                Logger.LogDebug($"ReadBatchBytes: CONTINUE - batchOffset {batchOffset} <= requestedOffset {offset} but lastOffset {lastOffset} < requestedOffset");
                return (false, false, default, bytesConsumed);
            });
    }

    public LogRecordBatch? ReadBatch(ulong offset)
    {
        return ReadBatchCore(
            offset,
            (data) =>
            {
                var batch = _batchReader.ReadBatch(data);
                var bytesConsumed = BatchHeaderSize + (int)BinaryPrimitives.ReadUInt32BigEndian(data[8..12]);

                if (offset >= batch.BaseOffset && offset <= batch.LastOffset)
                    return (true, false, batch, bytesConsumed);

                if (batch.BaseOffset > offset)
                    return (false, true, default, bytesConsumed);

                return (false, false, default, bytesConsumed);
            });
    }

    private T? ReadBatchCore<T>(
        ulong offset,
        Func<ReadOnlySpan<byte>, (bool found, bool passed, T value, int bytesConsumed)> readNext)
    {
        if (offset < _segment.BaseOffset)
        {
            throw new SegmentReaderException(
                "Start read offset is smaller than segment base offset, trying to read from wrong file");
        }
        
        var currentLength = RandomAccess.GetLength(_logHandle);
        var position = (long)FindPositionForOffset(offset);
        
        Logger.LogDebug($"ReadBatchCore: seeking to position {position} for offset {offset}, file length={currentLength}");

        var iterations = 0;
        while (position < currentLength)
        {
            iterations++;
            Logger.LogDebug($"ReadBatchCore: iteration {iterations}, position={position}, length={currentLength}");
            try
            {
                var batchSize = ReadBatchSize(position);
                if (batchSize == null)
                    break;

                var buffer = new byte[batchSize.Value];
                var bytesRead = RandomAccess.Read(_logHandle, buffer, position);
                if (bytesRead < batchSize.Value)
                    break;

                var (found, passed, value, bytesConsumed) = readNext(buffer);
                position += bytesConsumed;

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

        Logger.LogDebug($"ReadBatchCore: returning NULL after {iterations} iterations, final position={position}, length={currentLength}");
        return default;
    }

    public IEnumerable<LogRecordBatch> ReadRange(ulong startOffset, ulong endOffset)
    {
        if (startOffset >= endOffset)
        {
            yield break;
        }

        var currentLength = RandomAccess.GetLength(_logHandle);
        var position = (long)FindPositionForOffset(startOffset);

        while (position < currentLength)
        {
            var batchSize = ReadBatchSize(position);
            if (batchSize == null)
                yield break;

            var buffer = new byte[batchSize.Value];
            var bytesRead = RandomAccess.Read(_logHandle, buffer, position);
            if (bytesRead < batchSize.Value)
                yield break;

            LogRecordBatch batch;
            try
            {
                batch = _batchReader.ReadBatch(buffer.AsSpan());
            }
            catch (EndOfStreamException)
            {
                yield break;
            }

            position += batchSize.Value;

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

    public IEnumerable<LogRecordBatch> ReadFromTimestamp(ulong timestamp)
    {
        var currentLength = RandomAccess.GetLength(_logHandle);
        var position = (long)FindPositionForTimestamp(timestamp);

        while (position < currentLength)
        {
            var batchSize = ReadBatchSize(position);
            if (batchSize == null)
                yield break;

            var buffer = new byte[batchSize.Value];
            var bytesRead = RandomAccess.Read(_logHandle, buffer, position);
            if (bytesRead < batchSize.Value)
                yield break;

            LogRecordBatch batch;
            try
            {
                batch = _batchReader.ReadBatch(buffer.AsSpan());
            }
            catch (EndOfStreamException)
            {
                yield break;
            }

            position += batchSize.Value;

            if (batch.BaseTimestamp >= timestamp)
            {
                yield return batch;
            }
        }
    }

    private int? ReadBatchSize(long position)
    {
        Span<byte> header = stackalloc byte[BatchHeaderSize];
        var bytesRead = RandomAccess.Read(_logHandle, header, position);
        if (bytesRead < BatchHeaderSize)
            return null;

        var batchLength = BinaryPrimitives.ReadUInt32BigEndian(header[8..12]);
        if (batchLength is 0 or > int.MaxValue)
            return null;

        return BatchHeaderSize + (int)batchLength;
    }

    private ulong FindPositionForOffset(ulong offset)
    {
        if (_indexHandle == null || RandomAccess.GetLength(_indexHandle) == 0)
            return 0;

        ulong relative = offset - _segment.BaseOffset;
        int entryCount = (int)(RandomAccess.GetLength(_indexHandle) / OffsetIndexEntry.Size);

        var bestEntry = IndexBinarySearch.Search(
            entryCount,
            readEntryAt: ReadIndexEntry,
            getKey: e => e.RelativeOffset,
            targetKey: relative
        );

        return bestEntry?.FilePosition ?? 0;
    }

    private ulong FindPositionForTimestamp(ulong timestamp)
    {
        if (_timeIndexHandle == null || RandomAccess.GetLength(_timeIndexHandle) == 0)
            return 0;

        int entryCount = (int)(RandomAccess.GetLength(_timeIndexHandle) / TimeIndexEntry.Size);

        var bestEntry = IndexBinarySearch.Search(
            entryCount,
            readEntryAt: ReadTimeIndexEntry,
            getKey: e => e.Timestamp,
            targetKey: timestamp
        );

        return bestEntry?.FilePosition ?? 0;
    }

    private OffsetIndexEntry ReadIndexEntry(int entryIndex)
    {
        var position = (long)entryIndex * OffsetIndexEntry.Size;
        Span<byte> buffer = stackalloc byte[OffsetIndexEntry.Size];
        RandomAccess.Read(_indexHandle!, buffer, position);
        return _offsetIndexReader.ReadFrom(buffer);
    }

    private TimeIndexEntry ReadTimeIndexEntry(int entryIndex)
    {
        var position = (long)entryIndex * TimeIndexEntry.Size;
        Span<byte> buffer = stackalloc byte[TimeIndexEntry.Size];
        RandomAccess.Read(_timeIndexHandle!, buffer, position);
        return _timeIndexReader.ReadFrom(buffer);
    }

    public ulong RecoverHighWaterMark()
    {
        var currentLength = RandomAccess.GetLength(_logHandle);
        
        if (currentLength == 0)
        {
            return _segment.BaseOffset;
        }

        long position = 0;
        ulong highWaterMark = _segment.BaseOffset;

        while (position < currentLength)
        {
            var batchSize = ReadBatchSize(position);
            if (batchSize == null)
                break;

            var buffer = new byte[batchSize.Value];
            var bytesRead = RandomAccess.Read(_logHandle, buffer, position);
            if (bytesRead < batchSize.Value)
                break;

            try
            {
                var (_, _, lastOffset, _) = _batchReader.ReadBatchBytesAndAdvance(buffer);
                highWaterMark = lastOffset + 1;
                position += batchSize.Value;
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

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _logHandle.Dispose();
        _indexHandle?.Dispose();
        _timeIndexHandle?.Dispose();

        _disposed = true;
        await ValueTask.CompletedTask;
    }
}
