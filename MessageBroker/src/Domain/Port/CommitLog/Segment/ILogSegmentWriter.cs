using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Segment;

public interface ILogSegmentWriter
{
    ValueTask AppendAsync(byte[] batch, ulong batchBaseOffset, ulong batchLastOffset,
        CancellationToken ct);

    bool ShouldRoll();
    ValueTask DisposeAsync();
}
