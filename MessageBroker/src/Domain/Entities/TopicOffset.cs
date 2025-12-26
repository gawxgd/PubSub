namespace MessageBroker.Domain.Entities;

public sealed record TopicOffset(string Topic, ulong Offset)
{
    public const byte Separator = (byte)':';
}

