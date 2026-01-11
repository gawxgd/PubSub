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
    private static readonly ConcurrentDictionary<string, bool> ShouldStopPublishingByTopic = new();

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
                    // Check if we should stop publishing (simulations completed)
                    if (ShouldStopPublishingByTopic.TryGetValue(topicName, out var shouldStop) && shouldStop)
                    {
                        return Response.Ok(); // Return OK but don't publish
                    }
                    
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

                    // Create cancellation token source - will be cancelled in WithClean when scenario ends
                    var cts = new CancellationTokenSource();
                    CancellationTokensByTopic.TryAdd(topicName, cts);
                    
                    // Start background task to stop publishers and subscribers after load simulations complete
                    // Total simulation time: 5min (warm-up load) + 3min (peak) + 5min (cool-down) = 13 minutes
                    // Plus 1 minute warm-up = 14 minutes total
                    // Add 10 seconds buffer to ensure simulations are done
                    var totalTestDuration = TimeSpan.FromMinutes(1) + TimeSpan.FromMinutes(5) + TimeSpan.FromMinutes(3) + TimeSpan.FromMinutes(5) + TimeSpan.FromSeconds(10);
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Starting auto-stop task for {cleanTopicName} (will stop after {totalTestDuration.TotalMinutes:F1} minutes)");
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await Task.Delay(totalTestDuration);
                            Console.WriteLine($"\n[{DateTime.UtcNow:HH:mm:ss}] ═══ Auto-stopping publishers and subscribers for {cleanTopicName} (test duration elapsed) ═══");
                            
                            // Set flag to stop publishing in scenario function
                            ShouldStopPublishingByTopic.TryAdd(topicName, true);
                            Console.WriteLine($"  ✓ Publishing stopped flag set for {cleanTopicName}");
                            
                            // Dispose publishers FIRST to stop background publishing tasks
                            if (PublishersByTopic.TryGetValue(topicName, out var autoPubs))
                            {
                                Console.WriteLine($"  Stopping {autoPubs.Count} publishers...");
                                var disposeTasks = new List<Task>();
                                foreach (var pub in autoPubs)
                                {
                                    if (pub is IAsyncDisposable d)
                                    {
                                        disposeTasks.Add(Task.Run(async () =>
                                        {
                                            try
                                            {
                                                await d.DisposeAsync();
                                            }
                                            catch (Exception ex)
                                            {
                                                Console.WriteLine($"  ⚠ Error disposing publisher: {ex.Message}");
                                            }
                                        }));
                                    }
                                }
                                await Task.WhenAny(Task.WhenAll(disposeTasks), Task.Delay(TimeSpan.FromSeconds(5)));
                                Console.WriteLine($"  ✓ Publishers stopped");
                            }
                            else
                            {
                                Console.WriteLine($"  ⚠ No publishers found for {cleanTopicName}");
                            }
                            
                            // Cancel subscribers
                            if (CancellationTokensByTopic.TryGetValue(topicName, out var autoCts) && !autoCts.Token.IsCancellationRequested)
                            {
                                autoCts.Cancel();
                                Console.WriteLine($"  ✓ Subscribers cancelled");
                            }
                            else
                            {
                                Console.WriteLine($"  ⚠ No cancellation token found for {cleanTopicName} or already cancelled");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  ⚠ Error in auto-stop task for {cleanTopicName}: {ex.Message}");
                            Console.WriteLine($"  StackTrace: {ex.StackTrace}");
                        }
                    });

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
                                // StartMessageProcessingAsync checks CancellationToken internally
                                // WaitAsync here just adds a timeout wrapper, but cancellation is handled by StartMessageProcessingAsync
                                await subscriber.StartMessageProcessingAsync();
                            }
                            catch (OperationCanceledException) 
                            {
                                // Expected when cancellation is requested
                            }
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
                Simulation.Inject(rate: 20, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(5)),
                Simulation.Inject(rate: 7000, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(3)),
                Simulation.Inject(rate: 20, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(5))
            )

            .WithClean(async context =>
            {
                var cleanupStart = DateTime.UtcNow;
                Console.WriteLine($"\n[{cleanupStart:HH:mm:ss}] ═══ CLEANUP STARTED for {cleanTopicName} ═══");

                try
                {
                    // Cancel all subscriber tasks immediately
                    CancellationTokenSource? cts = null;
                    if (CancellationTokensByTopic.TryRemove(topicName, out cts))
                    {
                        Console.WriteLine($"  [1/6] Cancelling subscriber tasks for {cleanTopicName}...");
                        if (!cts.Token.IsCancellationRequested)
                        {
                            cts.Cancel();
                            Console.WriteLine($"  [1/6] ✓ Subscriber cancellation token cancelled");
                        }
                        else
                        {
                            Console.WriteLine($"  [1/6] ✓ Subscriber cancellation token already cancelled");
                        }
                        cts.Dispose();
                    }
                    else
                    {
                        Console.WriteLine($"  [1/6] ⚠ No subscriber cancellation token found for {cleanTopicName}");
                    }
                    
                    // Dispose publishers FIRST to stop background tasks (ProcessChannelAsync, ReceiveResponsesAsync)
                    // This ensures publishers stop publishing immediately
                    if (PublishersByTopic.TryRemove(topicName, out var pubs))
                    {
                        Console.WriteLine($"  [2/6] Disposing {pubs.Count} publishers to stop background tasks (max 2s each)...");
                        var disposeTasks = new List<Task>();
                        foreach (var pub in pubs)
                        {
                            if (pub is IAsyncDisposable d)
                            {
                                disposeTasks.Add(Task.Run(async () =>
                                {
                                    try
                                    {
                                        var disposeTask = d.DisposeAsync().AsTask();
                                        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(2));
                                        var completedTask = await Task.WhenAny(disposeTask, timeoutTask);
                                        
                                        if (completedTask == timeoutTask)
                                        {
                                            Console.WriteLine($"  ⚠ Publisher dispose timed out after 2s");
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"  ⚠ Error disposing publisher: {ex.Message}");
                                    }
                                }));
                            }
                        }
                        await Task.WhenAny(Task.WhenAll(disposeTasks), Task.Delay(TimeSpan.FromSeconds(5)));
                        Console.WriteLine($"  [2/6] ✓ Publishers disposal completed");
                    }

                    // Wait for subscriber tasks with shorter timeout - don't wait forever
                    if (SubscriberTasksByTopic.TryRemove(topicName, out var tasks))
                    {
                        Console.WriteLine($"  [3/6] Waiting for {tasks.Count} subscriber tasks (max 5s)...");
                        try 
                        { 
                            // Check task status
                            var completedCount = tasks.Count(t => t.IsCompleted);
                            var faultedCount = tasks.Count(t => t.IsFaulted);
                            var cancelledCount = tasks.Count(t => t.IsCanceled);
                            Console.WriteLine($"    Status: {completedCount} completed, {faultedCount} faulted, {cancelledCount} cancelled, {tasks.Count - completedCount - faultedCount - cancelledCount} running");
                            
                            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
                            var allTasksTask = Task.WhenAll(tasks);
                            var completedTask = await Task.WhenAny(allTasksTask, timeoutTask);
                            
                            if (completedTask == timeoutTask)
                            {
                                Console.WriteLine($"  ⚠ Subscriber tasks did not complete within 5s timeout - continuing anyway");
                                // Log which tasks are still running
                                for (int i = 0; i < tasks.Count; i++)
                                {
                                    if (!tasks[i].IsCompleted)
                                    {
                                        Console.WriteLine($"    Task {i}: Status={tasks[i].Status}, IsCompleted={tasks[i].IsCompleted}");
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine($"  ✓ All subscriber tasks completed");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  ⚠ Error waiting for subscriber tasks: {ex.Message}");
                            Console.WriteLine($"  StackTrace: {ex.StackTrace}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"  [2/5] ⚠ No subscriber tasks found for {cleanTopicName}");
                    }

                    // Dispose subscribers with timeout - do in parallel to speed up
                    if (SubscribersByTopic.TryRemove(topicName, out var subs))
                    {
                        Console.WriteLine($"  [4/6] Disposing {subs.Count} subscribers (max 2s each)...");
                        var disposeTasks = new List<Task>();
                        foreach (var sub in subs)
                        {
                            if (sub is IAsyncDisposable d)
                            {
                                disposeTasks.Add(Task.Run(async () =>
                                {
                                    try
                                    {
                                        var disposeTask = d.DisposeAsync().AsTask();
                                        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(2));
                                        var completedTask = await Task.WhenAny(disposeTask, timeoutTask);
                                        
                                        if (completedTask == timeoutTask)
                                        {
                                            Console.WriteLine($"  ⚠ Subscriber dispose timed out after 2s");
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"  ⚠ Error disposing subscriber: {ex.Message}");
                                    }
                                }));
                            }
                        }
                        await Task.WhenAny(Task.WhenAll(disposeTasks), Task.Delay(TimeSpan.FromSeconds(5)));
                        Console.WriteLine($"  ✓ Subscribers disposal completed");
                    }

                    // Final statistics
                    Console.WriteLine($"  [5/6] Collecting statistics...");
                    var pubCount = PublishedMessagesByTopic.TryGetValue(topicName, out var pubDict) ? pubDict.Count : 0;
                    var recCount = ReceivedMessagesByTopic.TryGetValue(topicName, out var recDict) ? recDict.Count : 0;
                    
                    var cleanupEnd = DateTime.UtcNow;
                    var cleanupDuration = (cleanupEnd - cleanupStart).TotalSeconds;
                    
                    Console.WriteLine($"  Published: {pubCount:N0}, Received: {recCount:N0}, Loss: {pubCount - recCount:N0}");
                    Console.WriteLine($"[{cleanupEnd:HH:mm:ss}] === Cleanup COMPLETE: {cleanTopicName} (took {cleanupDuration:F1}s) ===");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  ✗ Cleanup error: {ex.Message}");
                    Console.WriteLine($"  StackTrace: {ex.StackTrace}");
                }
            });

            scenarios.Add(scenario);
        }

        return scenarios.ToArray();
    }
}
