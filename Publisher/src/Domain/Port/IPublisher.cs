using Publisher.Domain.Exceptions;

namespace Publisher.Domain.Port;

public interface IPublisher<in T>
{
    Task CreateConnection();
    
    Task PublishAsync(T message);
    
    Task<bool> WaitForAcknowledgmentsAsync(int count, TimeSpan timeout);
    
    long AcknowledgedCount { get; }
}
