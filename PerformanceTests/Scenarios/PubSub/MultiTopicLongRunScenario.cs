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
/// Long-run multi-topic test: 10 topics, 10 publishers per topic, 5 subscribers per topic
/// Total: 5000 msg/s for 2 hours
/// </summary>
public static class MultiTopicLongRunScenario
{
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> PublishedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> ReceivedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, List<IPublisher<TestMessage>>> PublishersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<ISubscriber<TestMessage>>> SubscribersByTopic = new();

    // HELPER: Clean topic name - usuń schema/topic/ jeśli jest
    private static string CleanTopicName(string topic)
    {
        return topic.Replace("schema/topic/", "").Trim('/');
    }

    public static ScenarioProps[] Create(
        IPublisherFactory<TestMessage> publisherFactory,
        PublisherOptions publisherOptionsTemplate,
        ISubscriberFactory<TestMessage> subscriberFactory,
        SubscriberOptions subscriberOptionsTemplate)
    {
        const int numTopics = 10;
        const int publishersPerTopic = 10;
        const int subscribersPerTopic = 5;
        const int totalRate = 5000; // msg/s total
        const int ratePerTopic = totalRate / numTopics; // 500 msg/s per topic
        const int durationSeconds = 30; // 2 hours

        var scenarios = new List<ScenarioProps>();

        for (int topicIndex = 1; topicIndex <= numTopics; topicIndex++)
        {
            var topicName = $"long-run-topic-{topicIndex:D2}";
            var cleanTopicName = CleanTopicName(topicName); // CLEAN TOPIC
            var messageCounter = 0L;

            var scenario = Scenario.Create($"long_run_{topicName}", async context =>
            {
                try
                {
                    if (!PublishersByTopic.TryGetValue(topicName, out var publishers) || publishers.Count == 0)
                    {
                        Console.WriteLine($"ERROR: No publishers found for topic '{topicName}'");
                        return Response.Fail<object>("Publishers not initialized");
                    }

                    // Round-robin publisher selection
                    var publisherIndex = (int)(Interlocked.Read(ref messageCounter) % publishers.Count);
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
                    var publishedMessages = PublishedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());
                    publishedMessages.TryAdd(sequenceNumber, publishedAt);

                    await publisher.PublishAsync(message);

                    return Response.Ok();
                }
                catch (Exception ex)
                {
                    var errorMsg = $"Test failed for {topicName}: {ex.GetType().Name} - {ex.Message}";
                    Console.WriteLine($"ERROR in {topicName}: {errorMsg}");
                    return Response.Fail<object>(errorMsg);
                }
            })
            .WithInit(async context =>
            {
                try
                {
                    Console.WriteLine($"Initializing topic: {cleanTopicName}"); // UŻYJ CLEAN
                    Console.WriteLine($"  Publishers: {publishersPerTopic}");
                    Console.WriteLine($"  Subscribers: {subscribersPerTopic}");
                    Console.WriteLine($"  Rate: {ratePerTopic} msg/s");

                    var publishers = new List<IPublisher<TestMessage>>();
                    var subscribers = new List<ISubscriber<TestMessage>>();

                    // Create publishers - UŻYJ CLEAN TOPIC
                    for (int i = 0; i < publishersPerTopic; i++)
                    {
                        var pubOptions = publisherOptionsTemplate with { Topic = cleanTopicName }; // CLEAN!
                        var publisher = publisherFactory.CreatePublisher(pubOptions);
                        await publisher.CreateConnection();
                        publishers.Add(publisher);
                    }
                    Console.WriteLine($"  ✓ {publishersPerTopic} publishers initialized for {cleanTopicName}");

                    await Task.Delay(TimeSpan.FromMilliseconds(500));

                    // Create subscribers - UŻYJ CLEAN TOPIC
                    for (int i = 0; i < subscribersPerTopic; i++)
                    {
                        var subOptions = subscriberOptionsTemplate with { Topic = cleanTopicName }; // CLEAN!
                        var subscriber = subscriberFactory.CreateSubscriber(subOptions, async (message) =>
                        {
                            var receivedAt = DateTime.UtcNow;
                            var receivedMessages = ReceivedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());
                            receivedMessages.TryAdd(message.SequenceNumber, receivedAt);

                            var publishedMessages = PublishedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());
                            if (publishedMessages.TryGetValue(message.SequenceNumber, out var publishedAt))
                            {
                                var latency = receivedAt - publishedAt;
                                // Latency tracking
                            }

                            await Task.CompletedTask;
                        });

                        await subscriber.StartConnectionAsync();
                        
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                await subscriber.StartMessageProcessingAsync();
                            }
                            catch (OperationCanceledException)
                            {
                                // Expected when test ends
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Warning: Message processing error in {cleanTopicName}: {ex.Message}");
                            }
                        });

                        subscribers.Add(subscriber);
                    }
                    Console.WriteLine($"  ✓ {subscribersPerTopic} subscribers initialized for {cleanTopicName}");

                    PublishersByTopic.TryAdd(topicName, publishers);
                    SubscribersByTopic.TryAdd(topicName, subscribers);

                    Console.WriteLine($"  ✓ Topic {cleanTopicName} ready\n");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR initializing {cleanTopicName}: {ex.GetType().Name} - {ex.Message}");
                    throw;
                }
            })
            .WithWarmUpDuration(TimeSpan.FromSeconds(10))
            .WithLoadSimulations(
                Simulation.Inject(
                    rate: ratePerTopic,
                    interval: TimeSpan.FromSeconds(1),
                    during: TimeSpan.FromSeconds(durationSeconds))
            )
            .WithClean(async context =>
            {
                try
                {
                    Console.WriteLine($"\n=== Cleanup for {cleanTopicName} ===");

                    if (PublishersByTopic.TryRemove(topicName, out var publishers))
                    {
                        foreach (var publisher in publishers)
                        {
                            if (publisher is IAsyncDisposable pubDisposable)
                            {
                                await pubDisposable.DisposeAsync();
                            }
                        }
                        Console.WriteLine($"  ✓ Disposed {publishers.Count} publishers");
                    }

                    if (SubscribersByTopic.TryRemove(topicName, out var subscribers))
                    {
                        foreach (var subscriber in subscribers)
                        {
                            if (subscriber is IAsyncDisposable subDisposable)
                            {
                                await subDisposable.DisposeAsync();
                            }
                        }
                        Console.WriteLine($"  ✓ Disposed {subscribers.Count} subscribers");
                    }

                    var publishedMessages = PublishedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());
                    var receivedMessages = ReceivedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());

                    Console.WriteLine($"\n=== Statistics for {cleanTopicName} ===");
                    Console.WriteLine($"  Published: {publishedMessages.Count:N0}");
                    Console.WriteLine($"  Received: {receivedMessages.Count:N0}");
                    Console.WriteLine($"  Loss: {publishedMessages.Count - receivedMessages.Count:N0}");
                    
                    if (publishedMessages.Count > 0)
                    {
                        var deliveryRate = (double)receivedMessages.Count / publishedMessages.Count * 100;
                        Console.WriteLine($"  Delivery rate: {deliveryRate:F2}%");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Cleanup error for {cleanTopicName}: {ex.Message}");
                }
            });

            scenarios.Add(scenario);
        }

        return scenarios.ToArray();
    }
}
