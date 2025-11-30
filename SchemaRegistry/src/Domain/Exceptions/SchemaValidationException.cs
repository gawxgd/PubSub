namespace SchemaRegistry.Domain.Exceptions;

public class SchemaValidationException : Exception
{
    public SchemaValidationException(string message) : base(message) { }
    public SchemaValidationException(string message, Exception inner) : base(message, inner) { }
}