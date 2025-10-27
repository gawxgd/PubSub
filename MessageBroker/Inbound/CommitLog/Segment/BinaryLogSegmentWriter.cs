using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog.Segment;

public sealed class BinaryLogSegmentWriter
    : ILogSegmentWriter
{
    private readonly FileStream _index;
    private readonly FileStream _log;
    private readonly FileStream _timeIndex;

    private readonly ILogRecordBatchWriter _batchWriter;
    private LogSegment _segment;
    private readonly ulong _maxSegmentBytes;
    private readonly uint _indexIntervalBytes;
    private ulong _bytesSinceLastIndex;
    private readonly uint _timeIndexIntervalMs;
    private ulong _lastTimeIndexTimestamp;

    public BinaryLogSegmentWriter(
        ILogRecordBatchWriter batchWriter,
        LogSegment segment,
        ulong maxSegmentBytes,
        uint indexIntervalBytes,
        uint timeIndexIntervalMs,
        uint fileBufferSize)
    {
        _batchWriter = batchWriter;
        _segment = segment;
        _maxSegmentBytes = maxSegmentBytes;
        _indexIntervalBytes = indexIntervalBytes;
        _timeIndexIntervalMs = timeIndexIntervalMs;
        var logDir = Path.GetDirectoryName(segment.LogPath);
        Directory.CreateDirectory(logDir!);

        _log = new FileStream(
            segment.LogPath,
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.Read,
            (int)fileBufferSize,
            FileOptions.None);

        _index = new FileStream(
            segment.IndexFilePath,
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.Read,
            (int)fileBufferSize,
            FileOptions.None);

        _timeIndex = new FileStream(
            segment.TimeIndexFilePath,
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.Read,
            (int)fileBufferSize,
            FileOptions.None);

        _log.Seek(0, SeekOrigin.End);
        _index.Seek(0, SeekOrigin.End);
        _timeIndex.Seek(0, SeekOrigin.End);
    }

    public bool ShouldRoll()
    {
        return
            (ulong)_log.Length >=
            _maxSegmentBytes; //ToDo we risk that the segment will be bigger because we dont accomodate the size of next batch
    }

    public async ValueTask AppendAsync(LogRecordBatch batch, CancellationToken ct = default)
    {
        var start = _log.Position;
        _batchWriter.WriteTo(batch, _log);
        await _log.FlushAsync(ct).ConfigureAwait(false);

        var written = (ulong)(_log.Position - start);
        _bytesSinceLastIndex += written;

        if (_bytesSinceLastIndex >= _indexIntervalBytes)
        {
            await WriteIndexAsync(start, batch, ct).ConfigureAwait(false);
            _bytesSinceLastIndex = 0;
        }

        var baseTs = batch.BaseTimestamp;
        if (_timeIndexIntervalMs > 0 &&
            (_lastTimeIndexTimestamp == 0 || baseTs - _lastTimeIndexTimestamp >= _timeIndexIntervalMs))
        {
            await WriteTimeIndexAsync(batch, ct).ConfigureAwait(false);
            _lastTimeIndexTimestamp = baseTs;
        }

        _segment = _segment with { NextOffset = batch.BaseOffset + (ulong)batch.Records.Count };
    }

    public async ValueTask DisposeAsync()
    {
        await _log.DisposeAsync().ConfigureAwait(false);
        await _index.DisposeAsync().ConfigureAwait(false);
        await _timeIndex.DisposeAsync().ConfigureAwait(false);
    }

    private async ValueTask WriteIndexAsync(long start, LogRecordBatch batch, CancellationToken ct)
    {
        var rel = batch.BaseOffset - _segment.BaseOffset;
        Span<byte> buf = stackalloc byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(buf[..8], rel);
        BinaryPrimitives.WriteUInt64BigEndian(buf.Slice(8, 8), (ulong)start);
        await _index.WriteAsync(buf.ToArray().AsMemory(), ct).ConfigureAwait(false);
        await _index.FlushAsync(ct).ConfigureAwait(false);
    }

    private async ValueTask WriteTimeIndexAsync(LogRecordBatch batch, CancellationToken ct)
    {
        var rel = batch.BaseOffset - _segment.BaseOffset;
        var ts = batch.BaseTimestamp;
        Span<byte> buf = stackalloc byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(buf[..8], ts);
        BinaryPrimitives.WriteUInt64BigEndian(buf.Slice(8, 8), rel);
        await _timeIndex.WriteAsync(buf.ToArray().AsMemory(), ct).ConfigureAwait(false);
        await _timeIndex.FlushAsync(ct).ConfigureAwait(false);
    }
}