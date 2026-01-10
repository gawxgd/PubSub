using System.Collections.Concurrent;
using System.Collections.Immutable;
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
/// Multi-topic test: 10 topics, 10 publishers per topic, 5 subscribers per topic
/// Uses same pattern as BDD E2E tests - create all publishers first, then all subscribers
/// </summary>
public static class MultiTopicPeakPerformanceScenario
{
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> PublishedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> ReceivedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, ImmutableList<IPublisher<TestMessage>>> PublishersByTopic = new();
    private static readonly ConcurrentDictionary<string, ImmutableList<ISubscriber<TestMessage>>> SubscribersByTopic = new();
    private static readonly ConcurrentDictionary<string, ImmutableList<Task>> SubscriberTasksByTopic = new();
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
                    // Get publishers list atomically and store locally to avoid race conditions
                    if (!PublishersByTopic.TryGetValue(topicName, out var publishers) || publishers == null)
                    {
                        return Response.Fail<object>($"Publishers not ready: {topicName}");
                    }

                    // Store count and list locally to avoid any race conditions
                    var publishersList = publishers; // Local reference
                    var publishersCount = publishersList.Count;
                    
                    if (publishersCount == 0)
                    {
                        return Response.Fail<object>($"Publishers not ready: {topicName} (empty list)");
                    }

                    // Round-robin publisher selection - use local count to avoid race condition
                    // Ensure index is non-negative and within bounds
                    var currentCounter = Interlocked.Read(ref messageCounter);
                    var publisherIndex = (int)(Math.Abs(currentCounter) % publishersCount);
                    
                    // Double-check bounds before accessing (defensive programming)
                    if (publisherIndex < 0 || publisherIndex >= publishersCount)
                    {
                        return Response.Fail<object>($"Invalid publisher index: {publisherIndex}, count: {publishersCount}, counter: {currentCounter}");
                    }
                    
                    // Use local reference to avoid any potential race conditions
                    var publisher = publishersList[publisherIndex];
                    
                    var sequenceNumber = Interlocked.Increment(ref messageCounter);

                    var message = new TestMessage
                    {
                        Id = (int)sequenceNumber,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Content = $"Test {topicName} #{sequenceNumber}",
                        Source = topicName,
                        SequenceNumber = sequenceNumber
                    };

                    PublishedMessagesByTopic
                        .GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>())
                        .TryAdd(sequenceNumber, DateTime.UtcNow);

                    await publisher.PublishAsync(message);

                    return Response.Ok();
                }
                catch (IndexOutOfRangeException ex)
                {
                    // Log detailed information for IndexOutOfRangeException
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] IndexOutOfRangeException in {topicName}: {ex.Message}");
                    Console.WriteLine($"  StackTrace: {ex.StackTrace}");
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"  InnerException: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                    }
                    return Response.Fail<object>($"{topicName}: {ex.GetType().Name}");
                }
                catch (Exception ex)
                {
                    return Response.Fail<object>($"{topicName}: {ex.GetType().Name}");
                }
            })
            
            .WithInit(async context =>
            {
                try
                {
                    Console.WriteLine($"\n[{DateTime.UtcNow:HH:mm:ss}] === INIT: {cleanTopicName} ===");

                    var cts = new CancellationTokenSource();
                    CancellationTokensByTopic.TryAdd(topicName, cts);

                    // ==================== STEP 1: Create ALL Publishers (like BDD test) ====================
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Creating {publishersPerTopic} publishers...");
                    var publishers = new List<IPublisher<TestMessage>>();
                    
                    for (int i = 0; i < publishersPerTopic; i++)
                    {
                        var pubOptions = publisherOptionsTemplate with { Topic = cleanTopicName };
                        var publisher = publisherFactory.CreatePublisher(pubOptions);
                        
                        // IMPORTANT: Each publisher gets connection sequentially
                        await publisher.CreateConnection();
                        publishers.Add(publisher);
                        
                        // Small delay to ensure factory cleanup
                        if (i < publishersPerTopic - 1)
                        {
                            await Task.Delay(50);
                        }
                    }
                    
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] ✓ {publishers.Count} publishers created");
                    
                    // Add to dictionary AFTER all are created
                    var immutablePublishers = publishers.ToImmutableList();
                    if (!PublishersByTopic.TryAdd(topicName, immutablePublishers))
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] WARNING: Failed to add publishers for {cleanTopicName} - key may already exist");
                    }

                    await Task.Delay(500);

                    // ==================== STEP 2: Create ALL Subscribers (like BDD test) ====================
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Creating {subscribersPerTopic} subscribers...");
                    var subscribers = new List<ISubscriber<TestMessage>>();
                    var subscriberTasks = new List<Task>();
                    
                    for (int i = 0; i < subscribersPerTopic; i++)
                    {
                        var subOptions = subscriberOptionsTemplate with { Topic = cleanTopicName };
                        var subscriber = subscriberFactory.CreateSubscriber(subOptions, async msg =>
                        {
                            ReceivedMessagesByTopic
                                .GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>())
                                .TryAdd(msg.SequenceNumber, DateTime.UtcNow);
                            await Task.CompletedTask;
                        });

                        await subscriber.StartConnectionAsync();

                        var idx = i;
                        var task = Task.Run(async () =>
                        {
                            try
                            {
                                await subscriber.StartMessageProcessingAsync().WaitAsync(cts.Token);
                            }
                            catch (OperationCanceledException) { }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Subscriber {idx} error: {ex.Message}");
                            }
                        }, cts.Token);

                        subscriberTasks.Add(task);
                        subscribers.Add(subscriber);
                    }
                    
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] ✓ {subscribers.Count} subscribers created");

                    // Add to dictionaries AFTER all are created
                    SubscribersByTopic.TryAdd(topicName, subscribers.ToImmutableList());
                    SubscriberTasksByTopic.TryAdd(topicName, subscriberTasks.ToImmutableList());

                    await Task.Delay(2000);

                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] === INIT COMPLETE: {cleanTopicName} ===\n");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] FATAL: {cleanTopicName} - {ex.Message}");
                    throw;
                }
            })

            .WithWarmUpDuration(TimeSpan.FromSeconds(60))
            
            .WithLoadSimulations(
                Simulation.Inject(rate: 20, interval: TimeSpan.FromSeconds(5), during: TimeSpan.FromMinutes(1)),
                Simulation.Inject(rate: 7000, interval: TimeSpan.FromSeconds(3), during: TimeSpan.FromMinutes(1)),
                Simulation.Inject(rate: 20, interval: TimeSpan.FromSeconds(5), during: TimeSpan.FromMinutes(1))
            )

            .WithClean(async context =>
            {
                Console.WriteLine($"\n[{DateTime.UtcNow:HH:mm:ss}] === Cleanup: {cleanTopicName} ===");

                try
                {
                    if (CancellationTokensByTopic.TryRemove(topicName, out var cts))
                    {
                        cts.Cancel();
                        cts.Dispose();
                    }

                    if (SubscriberTasksByTopic.TryRemove(topicName, out var tasks))
                    {
                        try { await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10)); }
                        catch { }
                    }

                    if (SubscribersByTopic.TryRemove(topicName, out var subs))
                    {
                        foreach (var sub in subs)
                        {
                            try
                            {
                                if (sub is IAsyncDisposable d)
                                    await d.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                            }
                            catch { }
                        }
                    }

                    if (PublishersByTopic.TryRemove(topicName, out var pubs))
                    {
                        foreach (var pub in pubs)
                        {
                            try
                            {
                                if (pub is IAsyncDisposable d)
                                    await d.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
                            }
                            catch { }
                        }
                    }

                    var pubCount = PublishedMessagesByTopic.TryGetValue(topicName, out var pubDict) ? pubDict.Count : 0;
                    var recCount = ReceivedMessagesByTopic.TryGetValue(topicName, out var recDict) ? recDict.Count : 0;
                    
                    Console.WriteLine($"  Published: {pubCount:N0}, Received: {recCount:N0}, Loss: {pubCount - recCount:N0}");
                }
                catch { }
            });

            scenarios.Add(scenario);
        }

        return scenarios.ToArray();
    }
}
