namespace Publisher.Outbound.Exceptions;

public class PublisherConnectionException : PublisherException
{
    public PublisherConnectionException()
    {
    }

    public PublisherConnectionException(string message)
        : base(message)
    {
    }

    public PublisherConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}