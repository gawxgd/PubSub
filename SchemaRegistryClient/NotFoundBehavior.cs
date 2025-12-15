namespace SchemaRegistryClient;

public enum NotFoundBehavior
{
    /// <summary>Return null when schema not found</summary>
    ReturnNull,
    /// <summary>Throw SchemaNotFoundException when schema not found</summary>
    ThrowException
}