namespace MessageBroker.Domain.Port;

public interface IHandleClientConnectionUseCase
{
    Task HandleConnection(CancellationToken cancellationToken);
}