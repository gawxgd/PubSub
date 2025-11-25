namespace MessageBroker.Domain.Entities.CommitLog;

public sealed record LogRecord(ulong Offset, ulong Timestamp, ReadOnlyMemory<byte> Payload);
