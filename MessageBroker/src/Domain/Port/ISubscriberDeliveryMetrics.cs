namespace MessageBroker.Domain.Port;

public interface ISubscriberDeliveryMetrics
{
    void RecordBatchSent(string topic, ulong batchBaseOffset, ulong lastOffset);

    long GetTotalSentBatches();
    long GetTotalSentRecords();
}

