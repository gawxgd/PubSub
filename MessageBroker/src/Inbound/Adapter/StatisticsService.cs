using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using Microsoft.Extensions.Options;

namespace MessageBroker.Inbound.Adapter;

public class StatisticsService(
    IConnectionRepository connectionRepository,
    ICommitLogFactory commitLogFactory,
    ISubscriberDeliveryMetrics subscriberDeliveryMetrics,
    IOptions<List<CommitLogTopicOptions>> commitLogTopicOptions)
    : IStatisticsService
{
    private readonly string[] _knownTopics =
        (commitLogTopicOptions.Value ?? new List<CommitLogTopicOptions>())
        .Select(t => t.Name)
        .Where(name => !string.IsNullOrWhiteSpace(name))
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
        .ToArray();

    public Statistics GetStatistics()
    {
        var connections = connectionRepository.GetAll();
        var publisherConnections = connections.Count(c => c.ConnectionType == ConnectionType.Publisher);
        var subscriberConnections = connections.Count(c => c.ConnectionType == ConnectionType.Subscriber);
        
        var topics = new List<TopicStatistics>();
        
        foreach (var topic in _knownTopics)
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
                topics.Add(new TopicStatistics
                {
                    Name = topic,
                    MessageCount = 0,
                    LastOffset = 0
                });
            }
        }
        
        var totalPublished = topics.Sum(t => t.MessageCount);
        var messagesPublished = (int)Math.Min(totalPublished, int.MaxValue);
        
        var messagesConsumed = (int)Math.Min(subscriberDeliveryMetrics.GetTotalSentRecords(), int.MaxValue);
        
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

