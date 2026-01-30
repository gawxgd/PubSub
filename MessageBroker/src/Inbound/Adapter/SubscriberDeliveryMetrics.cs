using System.Collections.Concurrent;

namespace MessageBroker.Inbound.Adapter;

public sealed class SubscriberDeliveryMetrics : MessageBroker.Domain.Port.ISubscriberDeliveryMetrics
{
    private long _totalSentBatches;
    private long _totalSentRecords;

    private readonly ConcurrentDictionary<string, long> _sentBatchesByTopic = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, long> _sentRecordsByTopic = new(StringComparer.OrdinalIgnoreCase);

    public void RecordBatchSent(string topic, ulong batchBaseOffset, ulong lastOffset)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            topic = "unknown";
        }

        var recordsInBatch = lastOffset >= batchBaseOffset
            ? checked((long)Math.Min(lastOffset - batchBaseOffset + 1, (ulong)long.MaxValue))
            : 0L;

        Interlocked.Increment(ref _totalSentBatches);
        Interlocked.Add(ref _totalSentRecords, recordsInBatch);

        _sentBatchesByTopic.AddOrUpdate(topic, 1L, (_, current) => current + 1L);
        _sentRecordsByTopic.AddOrUpdate(topic, recordsInBatch, (_, current) => current + recordsInBatch);
    }

    public long GetTotalSentBatches() => Interlocked.Read(ref _totalSentBatches);

    public long GetTotalSentRecords() => Interlocked.Read(ref _totalSentRecords);
}

