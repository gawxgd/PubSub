using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.RecordBatch;

public interface ILogRecordBatchWriter
{
    public void WriteTo(LogRecordBatch recordBatch, Stream stream);
}
