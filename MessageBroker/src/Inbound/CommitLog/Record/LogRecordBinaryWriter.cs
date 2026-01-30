using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Util;

namespace MessageBroker.Inbound.CommitLog.Record;

public class LogRecordBinaryWriter : ILogRecordWriter
{
    public void WriteTo(LogRecord record, BinaryWriter bw, ulong batchBaseTimestamp)
    {
        bw.Write((ulong)record.Offset);

        var timestampDelta = record.Timestamp - batchBaseTimestamp;
        var totalSize = record.Payload.Length + sizeof(ulong);

        bw.Write((uint)totalSize);
        bw.Write((ulong)timestampDelta);
        bw.Write(record.Payload.Span);
    }
}
