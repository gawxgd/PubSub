using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Inbound.Adapter;

public class StatisticsService(IConnectionRepository connectionRepository, ICommitLogFactory commitLogFactory) 
    : IStatisticsService
{
    public Statistics GetStatistics()
    {
        var connections = connectionRepository.GetAll();
        var publisherConnections = connections.Count(c => c.ConnectionType == ConnectionType.Publisher);
        var subscriberConnections = connections.Count(c => c.ConnectionType == ConnectionType.Subscriber);
        
        // Get statistics for known topics
        var topics = new List<TopicStatistics>();
        var knownTopics = new[] { "default" }; // TODO: Get from configuration
        
        foreach (var topic in knownTopics)
        {
            try
            {
                var reader = commitLogFactory.GetReader(topic);
                var highWaterMark = reader.GetHighWaterMark();
                topics.Add(new TopicStatistics
                {
                    Name = topic,
                    MessageCount = (long)highWaterMark,
                    LastOffset = highWaterMark
                });
            }
            catch
            {
                // Topic might not exist yet
                topics.Add(new TopicStatistics
                {
                    Name = topic,
                    MessageCount = 0,
                    LastOffset = 0
                });
            }
        }
        
        // Messages published = high water mark for default topic
        var messagesPublished = topics.FirstOrDefault(t => t.Name == "default")?.MessageCount ?? 0;
        
        // Messages consumed - we don't track this separately yet, so use published as approximation
        // TODO: Track consumed messages separately
        var messagesConsumed = messagesPublished; // Placeholder
        
        return new Statistics
        {
            MessagesPublished = (int)messagesPublished,
            MessagesConsumed = (int)messagesConsumed,
            ActiveConnections = connections.Count,
            PublisherConnections = publisherConnections,
            SubscriberConnections = subscriberConnections,
            Topics = topics.ToArray(),
            LastUpdate = DateTime.UtcNow
        };
    }
}

