namespace Publisher.Configuration.Exceptions;

public enum PublisherFactoryErrorCode
{
    InvalidUri,
    UnsupportedScheme,
    InvalidPort,
    QueueSizeExceeded,
    Unknown
}

public class PublisherFactoryException(
    string message,
    PublisherFactoryErrorCode errorCode = PublisherFactoryErrorCode.Unknown,
    Exception? innerException = null)
    : Exception(message, innerException)
{
    public PublisherFactoryErrorCode ErrorCode { get; } = errorCode;

    public override string ToString()
    {
        return $"{base.ToString()}\nErrorCode: {ErrorCode}";
    }
}