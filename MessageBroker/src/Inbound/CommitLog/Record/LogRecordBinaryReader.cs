using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Record;

namespace MessageBroker.Inbound.CommitLog.Record;

public class LogRecordBinaryReader : ILogRecordReader
{
    public LogRecord ReadFrom(Stream stream, ulong baseTimestamp)
    {
        Span<byte> buffer = stackalloc byte[8];

        stream.ReadExactly(buffer[..8]);
        var offset = BinaryPrimitives.ReadUInt64BigEndian(buffer);

        stream.ReadExactly(buffer[..4]);
        var totalSize = BinaryPrimitives.ReadUInt32BigEndian(buffer);

        stream.ReadExactly(buffer[..8]);
        var timestampDelta = BinaryPrimitives.ReadUInt64BigEndian(buffer);
        var timestamp = baseTimestamp + timestampDelta;

        var payloadLength = totalSize - sizeof(ulong);
        var payload = new byte[payloadLength];
        stream.ReadExactly(payload);

        return new LogRecord(offset, timestamp, payload);
    }
}
