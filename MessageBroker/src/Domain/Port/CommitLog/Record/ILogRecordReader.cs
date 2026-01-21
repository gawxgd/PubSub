using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Record;

public interface ILogRecordReader
{
    LogRecord ReadFrom(Stream stream, ulong baseTimestamp);
}
