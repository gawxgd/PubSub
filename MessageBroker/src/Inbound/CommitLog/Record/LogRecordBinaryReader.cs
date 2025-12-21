using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Util;

namespace MessageBroker.Inbound.CommitLog.Record;

public class LogRecordBinaryReader : ILogRecordReader
{
    public LogRecord ReadFrom(BinaryReader br, ulong baseTimestamp)
    {
        var offset = br.ReadUInt64();
        var totalSize = br.ReadUInt32();

        var timestampDelta = br.ReadUInt64();
        var timestamp = baseTimestamp + timestampDelta;

        var payloadLength = totalSize - sizeof(ulong);
        var payload = br.ReadBytes((int)payloadLength);

        return new LogRecord(offset, timestamp, payload);
    }
}