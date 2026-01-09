using System.Collections.Concurrent;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Models;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Subscriber.Configuration.Options;
using Subscriber.Domain;

namespace PerformanceTests.Scenarios;

public static class MultiTopicLongRunScenario
{
    private static readonly ConcurrentDictionary<string, List<IPublisher<TestMessage>>> PublishersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<ISubscriber<TestMessage>>> SubscribersByTopic = new();
    private static readonly ConcurrentDictionary<string, CancellationTokenSource> SubscriberCtsByTopic = new();
    private static readonly ConcurrentDictionary<string, List<Task>> SubscriberTasksByTopic = new();

    private static readonly ConcurrentDictionary<string, long> PublishedCount = new();
    private static readonly ConcurrentDictionary<string, long> ReceivedCount = new();

    private static string CleanTopic(string topic)
        => topic.Replace("schema/topic/", "").Trim('/');

    public static ScenarioProps[] Create(
        IPublisherFactory<TestMessage> publisherFactory,
        PublisherOptions publisherOptionsTemplate,
        ISubscriberFactory<TestMessage> subscriberFactory,
        SubscriberOptions subscriberOptionsTemplate)
    {
        const int numTopics = 10;
        const int publishersPerTopic = 10;
        const int subscribersPerTopic = 5;
        const int totalRate = 5000;
        const int ratePerTopic = totalRate / numTopics; // 500 per topic
        const int durationSeconds = 20; // 2 hours

        var scenarios = new List<ScenarioProps>();

        for (int i = 1; i <= numTopics; i++)
        {
            var rawTopic = $"long-run-topic-{i:D2}";
            var topic = CleanTopic(rawTopic);
            var messageCounter = 0L;

            var scenario = Scenario.Create($"long_run_{topic}", async context =>
            {
                try
                {
                    if (!PublishersByTopic.TryGetValue(topic, out var publishers))
                        return Response.Fail<object>("Publishers not initialized");

                    var index = (int)(Interlocked.Read(ref messageCounter) % publishers.Count);
                    var publisher = publishers[index];

                    var seq = Interlocked.Increment(ref messageCounter);

                    var msg = new TestMessage
                    {
                        Id = (int)seq,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Content = $"msg {seq}",
                        Source = topic,
                        SequenceNumber = seq
                    };

                    await publisher.PublishAsync(msg);
                    PublishedCount.AddOrUpdate(topic, 1, (_, v) => v + 1);

                    return Response.Ok();
                }
                catch (Exception ex)
                {
                    return Response.Fail<object>(ex.Message);
                }
            })
            .WithInit(async ctx =>
            {
                Console.WriteLine($"[INIT] {topic} - {publishersPerTopic}P + {subscribersPerTopic}S @ {ratePerTopic} msg/s");

                var publishers = new List<IPublisher<TestMessage>>();
                var subscribers = new List<ISubscriber<TestMessage>>();
                var cts = new CancellationTokenSource();

                SubscriberCtsByTopic[topic] = cts;

                // Publishers
                for (int p = 0; p < publishersPerTopic; p++)
                {
                    var opts = publisherOptionsTemplate with { Topic = topic };
                    var pub = publisherFactory.CreatePublisher(opts);
                    await pub.CreateConnection();
                    publishers.Add(pub);
                }
                Console.WriteLine($"  ✓ {publishersPerTopic} publishers ready");

                await Task.Delay(500); // Let publishers settle

                // Subscribers
                var tasks = new List<Task>();
                SubscriberTasksByTopic[topic] = tasks;

                for (int s = 0; s < subscribersPerTopic; s++)
                {
                    var opts = subscriberOptionsTemplate with { Topic = topic };

                    var sub = subscriberFactory.CreateSubscriber(
                        opts,
                        async msg =>
                        {
                            ReceivedCount.AddOrUpdate(topic, 1, (_, v) => v + 1);
                            await Task.CompletedTask;
                        });

                        await subscriber.StartConnectionAsync();
                        
                        // StartMessageProcessingAsync has its own internal loop - just call it once
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

                PublishersByTopic[topic] = publishers;
                SubscribersByTopic[topic] = subscribers;

                Console.WriteLine($"  ✓ {topic} initialized\n");
            })
            .WithWarmUpDuration(TimeSpan.FromSeconds(10))
            .WithLoadSimulations(
                Simulation.Inject(
                    rate: ratePerTopic,
                    interval: TimeSpan.FromSeconds(1),
                    during: TimeSpan.FromSeconds(durationSeconds)
                )
            )
            .WithClean(async ctx =>
{
    Console.WriteLine($"\n[CLEANUP] {topic}");

        if (SubscribersByTopic.TryRemove(topicName, out var subscribers))
        {
            Console.WriteLine($"  Stopping {subscribers.Count} subscribers...");
            
            var disposeTasks = subscribers.Select(async subscriber =>
            {
                if (subscriber is IAsyncDisposable subDisposable)
                {
                    try { await subDisposable.DisposeAsync(); }
                    catch (Exception ex) { Console.WriteLine($"     Error disposing subscriber: {ex.Message}"); }
                }
            });
            
            await Task.WhenAll(disposeTasks);
            Console.WriteLine($"  ✓ Disposed {subscribers.Count} subscribers");
        }

        // POTEM PUBLISHERS
        if (PublishersByTopic.TryRemove(topicName, out var publishers))
        {
            Console.WriteLine($"  Stopping {publishers.Count} publishers...");
            
            var pubDisposeTasks = publishers.Select(async publisher =>
            {
                if (publisher is IAsyncDisposable pubDisposable)
                {
                    try { await pubDisposable.DisposeAsync(); }
                    catch (Exception ex) { Console.WriteLine($"     Error disposing publisher: {ex.Message}"); }
                }
            });
            
            await Task.WhenAll(pubDisposeTasks);
            Console.WriteLine($"  ✓ Disposed {publishers.Count} publishers");
        }

    // 6. Stats
    var sent = PublishedCount.GetValueOrDefault(topic);
    var recv = ReceivedCount.GetValueOrDefault(topic);

    Console.WriteLine($"\n[STATS] {topic}");
    Console.WriteLine($"  Published: {sent:N0}");
    Console.WriteLine($"  Received:  {recv:N0}");
    Console.WriteLine($"  Loss:      {sent - recv:N0}");

    if (sent > 0)
    {
        Console.WriteLine($"  Delivery:  {(double)recv / sent * 100:F2}%");
    }

    Console.WriteLine($"[CLEANUP DONE] {topic}\n");
});

            scenarios.Add(scenario);
        }

        return scenarios.ToArray();
    }
}
