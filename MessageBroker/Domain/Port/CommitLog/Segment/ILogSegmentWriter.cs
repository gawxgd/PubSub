using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Segment;

public interface ILogSegmentWriter
{
    ValueTask AppendAsync(LogRecordBatch batch, CancellationToken ct = default);
    bool ShouldRoll();

    ValueTask DisposeAsync();
}