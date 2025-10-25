using System.Net.Sockets;

public class SubscriberConnectionException(string message, SocketException socketException) : Exception(message);