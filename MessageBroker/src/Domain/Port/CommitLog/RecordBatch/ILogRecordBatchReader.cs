using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.RecordBatch;

public interface ILogRecordBatchReader
{
    public LogRecordBatch ReadBatch(Stream stream);

    public ulong ReadBatchOffset(Stream steam);
}