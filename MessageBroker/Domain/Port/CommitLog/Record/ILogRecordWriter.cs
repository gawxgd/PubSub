using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Record;

public interface ILogRecordWriter
{
    void WriteTo(LogRecord record, BinaryWriter bw, ulong batchBaseTimestamp);
}