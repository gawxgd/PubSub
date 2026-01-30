namespace Publisher.Outbound.Exceptions;

public class PublisherException : Exception
{
    public PublisherException()
    {
    }

    public PublisherException(string message)
        : base(message)
    {
    }

    public PublisherException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
