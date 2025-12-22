namespace MessageBroker.Domain.Port;

public interface IStatisticsService
{
    Statistics GetStatistics();
}

public record Statistics
{
    public int MessagesPublished { get; init; }
    public int MessagesConsumed { get; init; }
    public int ActiveConnections { get; init; }
    public int PublisherConnections { get; init; }
    public int SubscriberConnections { get; init; }
    public TopicStatistics[] Topics { get; init; } = Array.Empty<TopicStatistics>();
    public DateTime LastUpdate { get; init; }
}

public record TopicStatistics
{
    public string Name { get; init; } = string.Empty;
    public long MessageCount { get; init; }
    public ulong LastOffset { get; init; }
}

