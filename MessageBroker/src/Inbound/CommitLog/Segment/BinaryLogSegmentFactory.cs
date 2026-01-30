using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;
using MessageBroker.Domain.Port.CommitLog.Index.Writer;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Infrastructure.Configuration.Options;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using Microsoft.Extensions.Options;

namespace MessageBroker.Inbound.CommitLog.Segment;

public sealed class BinaryLogSegmentFactory(
    ILogRecordBatchWriter batchWriter,
    ILogRecordBatchReader batchReader,
    IOffsetIndexWriter offsetIndexWriter,
    IOffsetIndexReader offsetIndexReader,
    ITimeIndexWriter timeIndexWriter,
    ITimeIndexReader timeIndexReader,
    IOptions<CommitLogOptions> options)
    : ILogSegmentFactory
{
    public ILogSegmentWriter CreateWriter(LogSegment segment)
    {
        var opt = options.Value;
        return new BinaryLogSegmentWriter(
            offsetIndexWriter,
            timeIndexWriter,
            segment,
            opt.MaxSegmentBytes,
            opt.IndexIntervalBytes,
            opt.TimeIndexIntervalMs,
            opt.FileBufferSize);
    }

    public ILogSegmentReader CreateReader(LogSegment segment)
    {
        var opt = options.Value;
        return new BinaryLogSegmentReader(
            batchReader,
            segment,
            offsetIndexReader,
            timeIndexReader,
            opt.ReaderLogBufferSize,
            opt.ReaderIndexBufferSize
        );
    }

    public LogSegment CreateLogSegment(string directory, ulong baseOffset)
    {
        var fileName = $"{baseOffset:D20}.log";
        var logPath = Path.Join(directory, fileName);
        var indexPath = Path.ChangeExtension(logPath, ".index");
        var timeIndexPath = Path.ChangeExtension(logPath, ".timeindex");
        return new LogSegment(logPath, indexPath, timeIndexPath, baseOffset, baseOffset);
    }
}
