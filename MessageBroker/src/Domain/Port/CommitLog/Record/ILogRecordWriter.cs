using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Record;

public interface ILogRecordWriter
{
    void WriteTo(LogRecord record, Stream stream, ulong batchBaseTimestamp);
}
