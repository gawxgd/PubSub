using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Util;

namespace MessageBroker.Inbound.CommitLog.Record;

public class LogRecordBinaryWriter : ILogRecordWriter
{
    public void WriteTo(LogRecord record, BinaryWriter bw, ulong batchBaseTimestamp)
    {
        bw.Write(record.Offset);
        // it is important that offset has constant size

        bw.WriteVarULong(record.Timestamp - batchBaseTimestamp);
        bw.WriteVarUInt((uint)record.Payload.Length);
        bw.Write(record.Payload);
    }
}