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

                    await sub.StartConnectionAsync();

                    // Start message processing with cancellation support
                    var t = Task.Run(async () =>
                    {
                        try
                        {
                            var processingTask = sub.StartMessageProcessingAsync();
                            var cancellationTask = Task.Delay(Timeout.Infinite, cts.Token);

                            // Wait for either processing to complete or cancellation
                            await Task.WhenAny(processingTask, cancellationTask);

                            if (cts.Token.IsCancellationRequested)
                            {
                                // Cancellation requested - exit gracefully
                                return;
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected on cancellation
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  ⚠ Subscriber error in {topic}: {ex.Message}");
                        }
                    });

                    tasks.Add(t);
                    subscribers.Add(sub);
                }
                Console.WriteLine($"  ✓ {subscribersPerTopic} subscribers ready");

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

    // 1. Cancel subscribers
    if (SubscriberCtsByTopic.TryRemove(topic, out var cts))
    {
        Console.WriteLine($"  Cancelling subscribers...");
        cts.Cancel();
    }

    // 2. Dispose subscribers FIRST
    if (SubscribersByTopic.TryRemove(topic, out var subs))
    {
        Console.WriteLine($"  Disposing {subs.Count} subscribers...");
        foreach (var s in subs)
        {
            try
            {
                if (s is IAsyncDisposable d)
                {
                    var disposeTask = d.DisposeAsync().AsTask();
                    var timeout = Task.Delay(2000); // 2s max
                    var completed = await Task.WhenAny(disposeTask, timeout);

                    if (completed == timeout)
                    {
                        Console.WriteLine($"    ⚠ Subscriber dispose timeout");
                    }
                }
            }
            catch { }
        }
        Console.WriteLine($"  ✓ Subscribers disposed");
    }

    // 3. NIE CZEKAJ NA TASKI - daj im 2s i koniec!
    if (SubscriberTasksByTopic.TryRemove(topic, out var tasks))
    {
        Console.WriteLine($"  Giving tasks 2s to finish...");

        var allTasksTask = Task.WhenAll(tasks);
        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(2));
        var completedTask = await Task.WhenAny(allTasksTask, timeoutTask);

        if (completedTask == allTasksTask)
        {
            Console.WriteLine($"  ✓ Tasks completed");
        }
        else
        {
            Console.WriteLine($"  ⚠ Tasks timeout - ABANDONING");
            // NIE CZEKAJ - porzuć taski!
        }
    }

    // 4. Cleanup CTS
    if (cts != null)
    {
        cts.Dispose();
    }

    // 5. Dispose publishers
    if (PublishersByTopic.TryRemove(topic, out var pubs))
    {
        Console.WriteLine($"  Disposing {pubs.Count} publishers...");
        foreach (var p in pubs)
        {
            try
            {
                if (p is IAsyncDisposable d)
                {
                    var disposeTask = d.DisposeAsync().AsTask();
                    var timeout = Task.Delay(2000);
                    await Task.WhenAny(disposeTask, timeout);
                }
            }
            catch { }
        }
        Console.WriteLine($"  ✓ Publishers disposed");
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
