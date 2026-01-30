namespace Subscriber.Domain;

public interface ISubscriber<T> where T : new()
{
    internal Task CreateConnection();
    Task StartMessageProcessingAsync();
    Task StartConnectionAsync(ulong? initialOffset = null);
    ulong? GetCommittedOffset();
}
