using System.Net.Sockets;

namespace Subscriber.Outbound.Exceptions;

public class SubscriberConnectionException(string message, SocketException? socketException, bool isRetriable = false)
    : Exception(message)
{
    public SocketException? SocketException { get; } = socketException;
    public bool IsRetriable { get; } = isRetriable;
}
