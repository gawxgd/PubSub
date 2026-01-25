using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.RecordBatch;

public interface ILogRecordBatchReader
{
   public LogRecordBatch ReadBatch(ReadOnlySpan<byte> data);

   public (byte[] batchBytes, ulong batchOffset, ulong lastOffset, int bytesConsumed) ReadBatchBytesAndAdvance(ReadOnlySpan<byte> data);
}
