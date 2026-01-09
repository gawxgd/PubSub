using System.Collections.Concurrent;
using NBomber.CSharp;
using PerformanceTests.Models;
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using Subscriber.Domain;
using NBomber.Contracts;

namespace PerformanceTests.Scenarios;

/// <summary>
/// Long-run multi-topic test:
/// 10 topics
/// 10 publishers per topic
/// 5 subscribers per topic
///
/// Load profile per topic:
/// - 15 min @ 50% (warm-up)
/// - 60 min @ 100%
/// - 15 min @ 150% (peak)
/// - 2 h   @ 80%  (long run)
///
/// Total base load: 5000 msg/s
/// </summary>
public static class MultiTopicPeakPerformanceScenario
{
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> PublishedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> ReceivedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, List<IPublisher<TestMessage>>> PublishersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<ISubscriber<TestMessage>>> SubscribersByTopic = new();

    private static string CleanTopicName(string topic) =>
        topic.Replace("schema/topic/", "").Trim('/');

    public static ScenarioProps[] Create(
        IPublisherFactory<TestMessage> publisherFactory,
        PublisherOptions publisherOptionsTemplate,
        ISubscriberFactory<TestMessage> subscriberFactory,
        SubscriberOptions subscriberOptionsTemplate)
    {
        const int numTopics = 10;
        const int publishersPerTopic = 10;
        const int subscribersPerTopic = 5;

        const int totalRate = 10000;          // msg/s total
        const int ratePerTopic = totalRate / numTopics; // 500 msg/s per topic

        var scenarios = new List<ScenarioProps>();

        for (int topicIndex = 1; topicIndex <= numTopics; topicIndex++)
        {
            var topicName = $"long-run-topic-{topicIndex:D2}";
            var cleanTopicName = CleanTopicName(topicName);
            var messageCounter = 0L;

            var scenario = Scenario.Create($"long_run_{topicName}", async context =>
            {
                try
                {
                    if (!PublishersByTopic.TryGetValue(topicName, out var publishers) || publishers.Count == 0)
                        return Response.Fail<object>("Publishers not initialized");

                    var publisherIndex =
                        (int)(Interlocked.Read(ref messageCounter) % publishers.Count);

                    var publisher = publishers[publisherIndex];
                    var sequenceNumber = Interlocked.Increment(ref messageCounter);

                    var message = new TestMessage
                    {
                        Id = (int)sequenceNumber,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Content = $"Long-run test {topicName} #{sequenceNumber}",
                        Source = topicName,
                        SequenceNumber = sequenceNumber
                    };

                    var publishedAt = DateTime.UtcNow;
                    PublishedMessagesByTopic
                        .GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>())
                        .TryAdd(sequenceNumber, publishedAt);

                    await publisher.PublishAsync(message);

                    return Response.Ok();
                }
                catch (Exception ex)
                {
                    return Response.Fail<object>(
                        $"{topicName}: {ex.GetType().Name} - {ex.Message}");
                }
            })

            // ================= INIT =================
            .WithInit(async context =>
            {
                Console.WriteLine($"\nInitializing topic: {cleanTopicName}");
                Console.WriteLine($"  Publishers : {publishersPerTopic}");
                Console.WriteLine($"  Subscribers: {subscribersPerTopic}");
                Console.WriteLine($"  Base rate  : {ratePerTopic} msg/s");

                var publishers = new List<IPublisher<TestMessage>>();
                var subscribers = new List<ISubscriber<TestMessage>>();

                for (int i = 0; i < publishersPerTopic; i++)
                {
                    var pubOptions = publisherOptionsTemplate with { Topic = cleanTopicName };
                    var publisher = publisherFactory.CreatePublisher(pubOptions);
                    await publisher.CreateConnection();
                    publishers.Add(publisher);
                }

                await Task.Delay(500);

                for (int i = 0; i < subscribersPerTopic; i++)
                {
                    var subOptions = subscriberOptionsTemplate with { Topic = cleanTopicName };
                    var subscriber = subscriberFactory.CreateSubscriber(subOptions, async msg =>
                    {
                        var receivedAt = DateTime.UtcNow;

                        ReceivedMessagesByTopic
                            .GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>())
                            .TryAdd(msg.SequenceNumber, receivedAt);

                        await Task.CompletedTask;
                    });

                    await subscriber.StartConnectionAsync();

                    _ = Task.Run(() => subscriber.StartMessageProcessingAsync());
                    subscribers.Add(subscriber);
                }

                PublishersByTopic[topicName] = publishers;
                SubscribersByTopic[topicName] = subscribers;

                Console.WriteLine($"  ✓ Topic {cleanTopicName} ready");
            })

            // ================= LOAD PROFILE =================
            .WithLoadSimulations(

                // 15 min – 50% (warm-up)
                Simulation.Inject(
                        rate: (int)(ratePerTopic * 0.5),
                        interval: TimeSpan.FromSeconds(1),
                        during: TimeSpan.FromMinutes(15)),

                // 60 min – 100%
                Simulation.Inject(
                        rate: ratePerTopic,
                        interval: TimeSpan.FromSeconds(1),
                        during: TimeSpan.FromMinutes(60)),

                // 15 min – 150% (peak)
                Simulation.Inject(
                        rate: (int)(ratePerTopic * 1.5),
                        interval: TimeSpan.FromSeconds(1),
                        during: TimeSpan.FromMinutes(15)),

                // 2 h – 80% (long run)
                Simulation.Inject(
                        rate: (int)(ratePerTopic * 0.8),
                        interval: TimeSpan.FromSeconds(1),
                        during: TimeSpan.FromHours(2))
            )

            // ================= CLEANUP =================
            .WithClean(async context =>
            {
                Console.WriteLine($"\n=== Cleanup for {cleanTopicName} ===");

                if (SubscribersByTopic.TryRemove(topicName, out var subscribers))
                {
                    foreach (var sub in subscribers)
                    {
                        if (sub is IAsyncDisposable d)
                        {
                            try { await d.DisposeAsync(); }
                            catch { /* ignore */ }
                        }
                    }
                }

                if (PublishersByTopic.TryRemove(topicName, out var publishers))
                {
                    foreach (var pub in publishers)
                    {
                        if (pub is IAsyncDisposable d)
                        {
                            try { await d.DisposeAsync(); }
                            catch { /* ignore */ }
                        }
                    }
                }

                Console.WriteLine($"✓ Cleanup done for {cleanTopicName}");
            });

            scenarios.Add(scenario);
        }

        return scenarios.ToArray();
    }
}
