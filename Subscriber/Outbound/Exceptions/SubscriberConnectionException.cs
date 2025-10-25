using System.Net.Sockets;

namespace Subscriber.Outbound.Exceptions;

public class SubscriberConnectionException(string message, SocketException? socketException) : Exception(message);