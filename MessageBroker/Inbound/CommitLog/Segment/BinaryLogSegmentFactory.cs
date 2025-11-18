using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Infrastructure.Configuration.Options;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using Microsoft.Extensions.Options;

namespace MessageBroker.Inbound.CommitLog.Segment;

public sealed class BinaryLogSegmentFactory(
    ILogRecordBatchWriter batchWriter,
    IOptions<CommitLogOptions> options)
    : ILogSegmentFactory
{
    public ILogSegmentWriter CreateWriter(LogSegment segment)
    {
        // ToDo add options validation
        var opt = options.Value;
        return new BinaryLogSegmentWriter(
            batchWriter,
            segment,
            opt.MaxSegmentBytes,
            opt.IndexIntervalBytes,
            opt.TimeIndexIntervalMs,
            opt.FileBufferSize);
    }

    public ILogSegmentReader CreateReader(LogSegment segment)
    {
        return new BinaryLogSegmentReader();
    }

    public LogSegment CreateLogSegment(string directory, ulong baseOffset)
    {
        var fileName = $"{baseOffset:D20}.log";
        var logPath = Path.Combine(directory, fileName);
        var indexPath = Path.ChangeExtension(logPath, ".index");
        var timeIndexPath = Path.ChangeExtension(logPath, ".timeindex");
        return new LogSegment(logPath, indexPath, timeIndexPath, baseOffset, baseOffset);
    }
}