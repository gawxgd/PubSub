namespace MessageBroker.Domain.Entities;

public sealed record MessageWithTopic(string Topic, byte[] Payload)
{
    public const byte Separator = (byte)':';
}
