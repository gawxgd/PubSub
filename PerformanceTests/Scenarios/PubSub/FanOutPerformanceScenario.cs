using System.Collections.Concurrent;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Models;
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using Subscriber.Domain;

namespace PerformanceTests.Scenarios;

/// <summary>
/// Fan-out performance test:
/// one publisher, multiple independent subscribers consuming the same topic.
/// </summary>
public static class FanOutPerformanceScenario
{
    private static readonly ConcurrentDictionary<long, DateTime> PublishedMessages = new();

    // subscriberId -> (sequenceNumber -> receivedAt)
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> ReceivedMessages
        = new();

    private static readonly ConcurrentDictionary<string, ISubscriber<TestMessage>> Subscribers = new();
    private static IPublisher<TestMessage>? Publisher;

    public static ScenarioProps Create(
        int subscriberCount,
        IPublisherFactory<TestMessage> publisherFactory,
        PublisherOptions publisherOptions,
        ISubscriberFactory<TestMessage> subscriberFactory,
        SubscriberOptions subscriberOptions)
    {
        var messageCounter = 0L;
        const string publisherKey = "fanout_publisher";

        return Scenario.Create($"fanout_{subscriberCount}_subscribers", async context =>
        {
            try
            {
                if (Publisher == null)
                {
                    return Response.Fail<object>("Publisher not initialized");
                }

                var sequenceNumber = Interlocked.Increment(ref messageCounter);

                var message = new TestMessage
                {
                    Id = (int)sequenceNumber,
                    SequenceNumber = sequenceNumber,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = $"Fan-out test message #{sequenceNumber}"
                };

                PublishedMessages.TryAdd(sequenceNumber, DateTime.UtcNow);

                await Publisher.PublishAsync(message);

                return Response.Ok();
            }
            catch (Exception ex)
            {
                var errorMsg = $"Fan-out publish failed: {ex.GetType().Name} - {ex.Message}";
                Console.WriteLine(errorMsg);
                return Response.Fail<object>(errorMsg);
            }
        })
        .WithInit(async context =>
        {
            try
            {
                Console.WriteLine(
                    $"Initializing fan-out scenario with {subscriberCount} subscribers");

                // Initialize publisher
                Publisher = publisherFactory.CreatePublisher(publisherOptions);
                await Publisher.CreateConnection();

                await Task.Delay(TimeSpan.FromMilliseconds(500));
                Console.WriteLine("Publisher initialized");

                // Initialize subscribers
                for (int i = 0; i < subscriberCount; i++)
                {
                    var subscriberId = $"subscriber-{i}";
                    var receivedMap = new ConcurrentDictionary<long, DateTime>();

                    ReceivedMessages.TryAdd(subscriberId, receivedMap);

                    var subscriber = subscriberFactory.CreateSubscriber(
                        subscriberOptions,
                        async message =>
                        {
                            receivedMap.TryAdd(
                                message.SequenceNumber,
                                DateTime.UtcNow);

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
                            // Expected on shutdown
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(
                                $"Warning: {subscriberId} processing error: {ex.Message}");
                        }
                    });

                    Subscribers.TryAdd(subscriberId, subscriber);
                    Console.WriteLine($"Subscriber '{subscriberId}' started");
                }

                Console.WriteLine("Fan-out scenario initialized successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"ERROR initializing fan-out scenario: {ex.GetType().Name} - {ex.Message}");
                throw;
            }
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(3))
        .WithLoadSimulations(
            Simulation.Inject(
                rate: 20,
                interval: TimeSpan.FromSeconds(1),
                during: TimeSpan.FromSeconds(20))
        )
        .WithClean(async context =>
        {
            Console.WriteLine("\nFan-out Test Statistics:");

            foreach (var (subscriberId, received) in ReceivedMessages)
            {
                var lost = PublishedMessages.Count - received.Count;
                Console.WriteLine(
                    $"  {subscriberId}: received={received.Count}, lost={lost}");
            }

            // Cleanup subscribers
            foreach (var (id, subscriber) in Subscribers)
            {
                try
                {
                    if (subscriber is IAsyncDisposable disposable)
                    {
                        var disposeTask = disposable.DisposeAsync().AsTask();
                        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
                        await Task.WhenAny(disposeTask, timeoutTask);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"Warning: error disposing {id}: {ex.Message}");
                }
            }

            Subscribers.Clear();
            ReceivedMessages.Clear();
            PublishedMessages.Clear();

            // Cleanup publisher
            if (Publisher is IAsyncDisposable pubDisposable)
            {
                var disposeTask = pubDisposable.DisposeAsync().AsTask();
                var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
                await Task.WhenAny(disposeTask, timeoutTask);
            }

            Publisher = null;
        });
    }
}
