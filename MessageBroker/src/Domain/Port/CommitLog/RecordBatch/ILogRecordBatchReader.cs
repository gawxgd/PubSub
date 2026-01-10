using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.RecordBatch;

public interface ILogRecordBatchReader
{
    public LogRecordBatch ReadBatch(Stream stream);

    public (byte[] batchBytes, ulong batchOffset, ulong lastOffset) ReadBatchBytesAndAdvance(Stream stream);
}