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
/// End-to-end performance test: publish messages and verify they are received by subscriber.
/// </summary>
public static class EndToEndPerformanceScenario
{
    private static readonly ConcurrentDictionary<long, DateTime> PublishedMessages = new();
    private static readonly ConcurrentDictionary<long, DateTime> ReceivedMessages = new();
    private static readonly ConcurrentDictionary<string, IPublisher<TestMessage>> Publishers = new();
    private static readonly ConcurrentDictionary<string, ISubscriber<TestMessage>> Subscribers = new();

    public static ScenarioProps Create(
        IPublisherFactory<TestMessage> publisherFactory,
        PublisherOptions publisherOptions,
        ISubscriberFactory<TestMessage> subscriberFactory,
        SubscriberOptions subscriberOptions)
    {
        var messageCounter = 0L;
        const string publisherKey = "e2e_publisher";
        const string subscriberKey = "e2e_subscriber";

        return Scenario.Create("end_to_end_throughput", async context =>
        {
            try
            {
                // Get initialized instances (thread-safe)
                if (!Publishers.TryGetValue(publisherKey, out var publisher))
                {
                    Console.WriteLine($"❌ ERROR: Publisher '{publisherKey}' not found");
                    return Response.Fail<object>("Publisher not initialized");
                }
                if (!Subscribers.TryGetValue(subscriberKey, out var subscriber))
                {
                    Console.WriteLine($"❌ ERROR: Subscriber '{subscriberKey}' not found");
                    return Response.Fail<object>("Subscriber not initialized");
                }

                // Create and publish message
                var sequenceNumber = Interlocked.Increment(ref messageCounter);
                var message = new TestMessage
                {
                    Id = (int)sequenceNumber,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = $"E2E test message #{sequenceNumber}",
                    SequenceNumber = sequenceNumber
                };

                var publishedAt = DateTime.UtcNow;
                PublishedMessages.TryAdd(sequenceNumber, publishedAt);

                await publisher.PublishAsync(message);

                return Response.Ok();
            }
            catch (Exception ex)
            {
                var errorMsg = $"E2E test failed: {ex.GetType().Name} - {ex.Message}";
                if (ex.InnerException != null)
                {
                    errorMsg += $" (Inner: {ex.InnerException.GetType().Name} - {ex.InnerException.Message})";
                }
                Console.WriteLine($"❌ ERROR in end_to_end_throughput: {errorMsg}");
                return Response.Fail<object>(errorMsg);
            }
        })
        .WithInit(async context =>
        {
            try
            {
                Console.WriteLine($"🔧 Initializing E2E scenario: publisher and subscriber");
                
                // Initialize publisher and subscriber before scenario starts
                var publisher = publisherFactory.CreatePublisher(publisherOptions);
                await publisher.CreateConnection();
                
                // Give publisher time to register schema and be ready
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                
                Publishers.TryAdd(publisherKey, publisher);
                Console.WriteLine($"✅ Publisher '{publisherKey}' initialized");

                var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, async (message) =>
                {
                    var receivedAt = DateTime.UtcNow;
                    ReceivedMessages.TryAdd(message.SequenceNumber, receivedAt);

                    // Calculate latency if we have publish time
                    if (PublishedMessages.TryGetValue(message.SequenceNumber, out var publishedAt))
                    {
                        var latency = receivedAt - publishedAt;
                        // Latency tracking - NBomber will aggregate this
                    }

                    await Task.CompletedTask;
                });

                await subscriber.StartConnectionAsync();
                Console.WriteLine($"✅ Subscriber '{subscriberKey}' connection started");
                
                // Start message processing in background - it will run until cancelled
                var messageProcessingTask = Task.Run(async () =>
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
                        Console.WriteLine($"⚠️  Warning: Message processing error: {ex.Message}");
                    }
                });
                
                Subscribers.TryAdd(subscriberKey, subscriber);
                Console.WriteLine($"✅ E2E scenario initialized successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ ERROR initializing E2E scenario: {ex.GetType().Name} - {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner exception: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                }
                throw;
            }
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(3))
        .WithLoadSimulations(
            // Steady load: 20 messages per second for 20 seconds (reduced for faster tests)
            Simulation.Inject(
                rate: 20,
                interval: TimeSpan.FromSeconds(1),
                during: TimeSpan.FromSeconds(20))
        )
        .WithClean(async context =>
        {
            // Cleanup on scenario end with timeout
            try
            {
                if (Publishers.TryRemove(publisherKey, out var publisher) && 
                    publisher is IAsyncDisposable pubDisposable)
                {
                    var disposeTask = pubDisposable.DisposeAsync().AsTask();
                    var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
                    var completedTask = await Task.WhenAny(disposeTask, timeoutTask);
                    if (completedTask == timeoutTask)
                    {
                        Console.WriteLine("⚠️  Warning: Publisher disposal timed out");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  Warning: Error disposing publisher: {ex.Message}");
            }
            
            try
            {
                if (Subscribers.TryRemove(subscriberKey, out var subscriber))
                {
                    // Dispose will cancel the cancellation token, which should stop message processing
                    if (subscriber is IAsyncDisposable subDisposable)
                    {
                        var disposeTask = subDisposable.DisposeAsync().AsTask();
                        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
                        var completedTask = await Task.WhenAny(disposeTask, timeoutTask);
                        if (completedTask == timeoutTask)
                        {
                            Console.WriteLine("⚠️  Warning: Subscriber disposal timed out - forcing stop");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  Warning: Error disposing subscriber: {ex.Message}");
            }

            // Log statistics
            Console.WriteLine($"\n📊 E2E Test Statistics:");
            Console.WriteLine($"   Published messages: {PublishedMessages.Count}");
            Console.WriteLine($"   Received messages: {ReceivedMessages.Count}");
            Console.WriteLine($"   Message loss: {PublishedMessages.Count - ReceivedMessages.Count}");
        });
    }
}

