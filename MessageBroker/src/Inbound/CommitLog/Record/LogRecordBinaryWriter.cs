using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Record;

namespace MessageBroker.Inbound.CommitLog.Record;

public class LogRecordBinaryWriter : ILogRecordWriter
{
    public void WriteTo(LogRecord record, Stream stream, ulong batchBaseTimestamp)
    {
        Span<byte> buffer = stackalloc byte[8];

        BinaryPrimitives.WriteUInt64BigEndian(buffer, (ulong)record.Offset);
        stream.Write(buffer[..8]);

        var timestampDelta = record.Timestamp - batchBaseTimestamp;
        var totalSize = record.Payload.Length + sizeof(ulong);

        BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)totalSize);
        stream.Write(buffer[..4]);

        BinaryPrimitives.WriteUInt64BigEndian(buffer, timestampDelta);
        stream.Write(buffer[..8]);

        stream.Write(record.Payload.Span);
    }
}
