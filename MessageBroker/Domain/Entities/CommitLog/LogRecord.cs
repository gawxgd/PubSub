namespace MessageBroker.Domain.Entities.CommitLog;

public sealed record LogRecord(ulong Offset, ulong Timestamp, byte[] Payload)
{
}