using System.IO.Pipelines;

namespace MessageBroker.Domain.Port;

public interface IMessageFramer
{
    byte[] FrameMessage(byte[] message);
}
