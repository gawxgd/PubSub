using System.Buffers.Binary;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;
using MessageBroker.Domain.Port.CommitLog.Index.Writer;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog.Segment;

public sealed class BinaryLogSegmentWriter
    : ILogSegmentWriter, IAsyncDisposable
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<BinaryLogSegmentWriter>(LogSource.MessageBroker);

    private readonly FileStream _index;
    private readonly FileStream _log;
    private readonly FileStream _timeIndex;

    private readonly IOffsetIndexWriter _indexWriter;
    private readonly ITimeIndexWriter _timeIndexWriter;

    private LogSegment _segment;
    private readonly ulong _maxSegmentBytes;
    private readonly uint _indexIntervalBytes;
    private ulong _bytesSinceLastIndex;
    private readonly uint _timeIndexIntervalMs;
    private ulong _lastTimeIndexTimestamp;

    public BinaryLogSegmentWriter(
        IOffsetIndexWriter indexWriter,
        ITimeIndexWriter timeIndexWriter,
        LogSegment segment,
        ulong maxSegmentBytes,
        uint indexIntervalBytes,
        uint timeIndexIntervalMs,
        uint fileBufferSize)
    {
        _indexWriter = indexWriter;
        _timeIndexWriter = timeIndexWriter;
        _segment = segment;
        _maxSegmentBytes = maxSegmentBytes;
        _indexIntervalBytes = indexIntervalBytes;
        _timeIndexIntervalMs = timeIndexIntervalMs;

        EnsureDirectoriesExists();

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

    private void EnsureDirectoriesExists()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_segment.LogPath)!);
        Directory.CreateDirectory(Path.GetDirectoryName(_segment.IndexFilePath)!);
        Directory.CreateDirectory(Path.GetDirectoryName(_segment.TimeIndexFilePath)!);
    }

    public bool ShouldRoll()
    {
        return
            (ulong)_log.Length >=
            _maxSegmentBytes; //ToDo we risk that the segment will be bigger because we dont accomodate the size of next batch
    }

    public async ValueTask AppendAsync(byte[] batch, ulong batchBaseOffset, ulong batchLastOffset,
        CancellationToken ct)
    {
        var start = _log.Position;

        await _log.WriteAsync(batch, 0, batch.Length, ct).ConfigureAwait(false);
        await _log.FlushAsync(ct).ConfigureAwait(false);

        var written = (ulong)(_log.Position - start);
        _bytesSinceLastIndex += written;

        if (_bytesSinceLastIndex >= _indexIntervalBytes)
        {
            await WriteIndexAsync(start, batchBaseOffset).ConfigureAwait(false);
            _bytesSinceLastIndex = 0;
        }

        //ToDo handle timestamps
        // var baseTs = batch.BaseTimestamp;
        // if (_timeIndexIntervalMs > 0 &&
        //     (_lastTimeIndexTimestamp == 0 || baseTs - _lastTimeIndexTimestamp >= _timeIndexIntervalMs))
        // {
        //     await WriteTimeIndexAsync(start, batch).ConfigureAwait(false);
        //     _lastTimeIndexTimestamp = baseTs;
        // }

        _segment = _segment with { NextOffset = batchLastOffset + 1 };
    }

    public async ValueTask DisposeAsync()
    {
        await _log.DisposeAsync().ConfigureAwait(false);
        await _index.DisposeAsync().ConfigureAwait(false);
        await _timeIndex.DisposeAsync().ConfigureAwait(false);
    }

    private async ValueTask WriteIndexAsync(long start, ulong batchBaseOffset)
    {
        var relativeOffset = batchBaseOffset - _segment.BaseOffset;
        var entry = new OffsetIndexEntry(relativeOffset, (ulong)start);
        await _indexWriter.WriteToAsync(entry, _index);
    }

    private async ValueTask WriteTimeIndexAsync(long start, LogRecordBatch batch)
    {
        var entry = new TimeIndexEntry(batch.BaseTimestamp, (ulong)start);
        await _timeIndexWriter.WriteToAsync(entry, _timeIndex);
    }
}