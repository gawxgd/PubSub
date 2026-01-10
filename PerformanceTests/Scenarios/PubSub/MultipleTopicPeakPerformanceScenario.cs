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
    
    // DODAJ: Tracking subscriber tasks i cancellation tokens
    private static readonly ConcurrentDictionary<string, List<Task>> SubscriberTasksByTopic = new();
    private static readonly ConcurrentDictionary<string, CancellationTokenSource> CancellationTokensByTopic = new();

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
        const int totalRate = 10000;
        const int ratePerTopic = totalRate / numTopics;

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
                    PublishedMessagesByTopic
                        .GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>())
                        .TryAdd(sequenceNumber, publishedAt);

                    await publisher.PublishAsync(message);

                    return Response.Ok();
                }
                catch (Exception ex)
                {
                    return Response.Fail<object>($"{topicName}: {ex.GetType().Name} - {ex.Message}");
                }
            })
            
            // ================= INIT =================
            .WithInit(async context =>
            {
                Console.WriteLine($"\n[{DateTime.UtcNow:HH:mm:ss}] Initializing topic: {cleanTopicName}");
                Console.WriteLine($"  Publishers : {publishersPerTopic}");
                Console.WriteLine($"  Subscribers: {subscribersPerTopic}");
                Console.WriteLine($"  Base rate  : {ratePerTopic} msg/s");

                var publishers = new List<IPublisher<TestMessage>>();
                var subscribers = new List<ISubscriber<TestMessage>>();
                var subscriberTasks = new List<Task>();
                
                // DODAJ: CancellationTokenSource dla tego topiku
                var cts = new CancellationTokenSource();
                CancellationTokensByTopic.TryAdd(topicName, cts);

                // Publishers
                for (int i = 0; i < publishersPerTopic; i++)
                {
                    var pubOptions = publisherOptionsTemplate with { Topic = cleanTopicName };
                    var publisher = publisherFactory.CreatePublisher(pubOptions);
                    await publisher.CreateConnection();
                    publishers.Add(publisher);
                }

                await Task.Delay(500);

                // Subscribers
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

                    // ZMIENIONE: Używaj CancellationToken
                    var subscriberTask = Task.Run(async () =>
                    {
                        try
                        {
                            // Jeśli twój StartMessageProcessingAsync nie przyjmuje CancellationToken,
                            // musisz to obsłużyć przez Task.WaitAsync lub timeout
                            await subscriber.StartMessageProcessingAsync();
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected during cleanup
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Subscriber cancelled for {cleanTopicName}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Subscriber error in {cleanTopicName}: {ex.Message}");
                        }
                    }, cts.Token);
                    
                    subscriberTasks.Add(subscriberTask);
                    subscribers.Add(subscriber);
                }

                PublishersByTopic[topicName] = publishers;
                SubscribersByTopic[topicName] = subscribers;
                SubscriberTasksByTopic[topicName] = subscriberTasks;

                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] ✓ Topic {cleanTopicName} ready");
            })

            // ================= LOAD PROFILE =================
            .WithLoadSimulations(
                Simulation.Inject(rate: 20, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(5)),
                Simulation.Inject(rate: 8000, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(3)),
                Simulation.Inject(rate: 20, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(5))
            )

            // ================= CLEANUP =================
            .WithClean(async context =>
            {
                var cleanupStart = DateTime.UtcNow;
                Console.WriteLine($"\n[{cleanupStart:HH:mm:ss}] === Cleanup START for {cleanTopicName} ===");

                try
                {
                    // 1. Cancel subscriber tasks
                    if (CancellationTokensByTopic.TryRemove(topicName, out var cts))
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Cancelling subscriber tasks...");
                        cts.Cancel();
                    }

                    // 2. Wait for subscriber tasks to finish (WITH TIMEOUT!)
                    if (SubscriberTasksByTopic.TryRemove(topicName, out var subscriberTasks))
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Waiting for {subscriberTasks.Count} subscriber tasks...");
                        
                        try
                        {
                            // Wait max 10 seconds for tasks to finish
                            await Task.WhenAll(subscriberTasks).WaitAsync(TimeSpan.FromSeconds(10));
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] ✓ All subscriber tasks finished");
                        }
                        catch (TimeoutException)
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Subscriber tasks timeout - forcing cleanup");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Error waiting for tasks: {ex.Message}");
                        }
                    }

                    // 3. Dispose subscribers (WITH TIMEOUT!)
                    if (SubscribersByTopic.TryRemove(topicName, out var subscribers))
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Disposing {subscribers.Count} subscribers...");
                        
                        for (int i = 0; i < subscribers.Count; i++)
                        {
                            try
                            {
                                if (subscribers[i] is IAsyncDisposable d)
                                {
                                    // Wrap DisposeAsync in timeout
                                    using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                                    await d.DisposeAsync().AsTask().WaitAsync(timeoutCts.Token);
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Subscriber {i} dispose timeout");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Subscriber {i} dispose error: {ex.Message}");
                            }
                        }
                    }

                    // 4. Dispose publishers (WITH TIMEOUT!)
                    if (PublishersByTopic.TryRemove(topicName, out var publishers))
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Disposing {publishers.Count} publishers...");
                        
                        for (int i = 0; i < publishers.Count; i++)
                        {
                            try
                            {
                                if (publishers[i] is IAsyncDisposable d)
                                {
                                    using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                                    await d.DisposeAsync().AsTask().WaitAsync(timeoutCts.Token);
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Publisher {i} dispose timeout");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Publisher {i} dispose error: {ex.Message}");
                            }
                        }
                    }

                    var cleanupEnd = DateTime.UtcNow;
                    var totalTime = (cleanupEnd - cleanupStart).TotalSeconds;
                    Console.WriteLine($"[{cleanupEnd:HH:mm:ss}] === Cleanup END for {cleanTopicName} in {totalTime:F1}s ===");
                    
                    // Stats
                    var publishedCount = PublishedMessagesByTopic.TryGetValue(topicName, out var pub) ? pub.Count : 0;
                    var receivedCount = ReceivedMessagesByTopic.TryGetValue(topicName, out var rec) ? rec.Count : 0;
                    Console.WriteLine($"  Published: {publishedCount}, Received: {receivedCount}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Fatal cleanup error for {cleanTopicName}: {ex.Message}");
                }
            });

            scenarios.Add(scenario);
        }

        return scenarios.ToArray();
    }
}
