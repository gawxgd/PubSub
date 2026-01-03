using System.Buffers;

namespace MessageBroker.Domain.Port;

public interface IMessageDeframer
{
    bool TryReadFramedMessage(ref ReadOnlySequence<byte> buffer, out byte[] message);
}