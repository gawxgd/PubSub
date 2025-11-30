using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Record;

public interface ILogRecordReader
{
    LogRecord ReadFrom(BinaryReader br, ulong baseTimestamp);
}