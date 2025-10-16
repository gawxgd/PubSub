namespace Publisher.Outbound.Port;

public interface IBrokerConnection<in T>
{
    Task PublishAsync(String topic, T message);
}