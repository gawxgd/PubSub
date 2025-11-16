using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Util;

namespace MessageBroker.Inbound.CommitLog.Record;

public class LogRecordBinaryReader : ILogRecordReader
{
    public LogRecord ReadFrom(BinaryReader br, ulong baseTimestamp)
    {
        var offset = br.ReadUInt64();
        var timestampDelta = br.ReadVarULong();
        var timestamp = baseTimestamp + timestampDelta;
        var payloadLength = br.ReadVarUInt();
        var payload = br.ReadBytes((int)payloadLength);
        return new LogRecord(offset, timestamp, payload);
    }
}