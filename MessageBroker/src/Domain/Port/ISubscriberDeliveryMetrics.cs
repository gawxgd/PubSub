namespace MessageBroker.Domain.Port;

/// <summary>
/// Metrics for broker -> subscriber deliveries (duplicates included).
/// </summary>
public interface ISubscriberDeliveryMetrics
{
    void RecordBatchSent(string topic, ulong batchBaseOffset, ulong lastOffset);

    long GetTotalSentBatches();
    long GetTotalSentRecords();
}

