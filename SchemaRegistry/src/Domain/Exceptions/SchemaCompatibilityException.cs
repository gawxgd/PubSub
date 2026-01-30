namespace SchemaRegistry.Domain.Exceptions;

public class SchemaCompatibilityException(string message) : Exception(message);
