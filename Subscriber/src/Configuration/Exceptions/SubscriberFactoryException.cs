namespace Subscriber.Configuration.Exceptions;

public enum SubscriberFactoryErrorCode
{
    InvalidUri,
    UnsupportedScheme,
    InvalidPort,
    MissingTopic
}

public class SubscriberFactoryException(string message, SubscriberFactoryErrorCode errorCode) : Exception(message)
{
    public SubscriberFactoryErrorCode ErrorCode { get; } = errorCode;
}
