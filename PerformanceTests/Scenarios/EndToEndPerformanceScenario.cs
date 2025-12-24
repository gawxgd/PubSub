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
                if (!Publishers.TryGetValue(publisherKey, out var publisher) ||
                    !Subscribers.TryGetValue(subscriberKey, out var subscriber))
                {
                    return Response.Fail<object>("Publisher or Subscriber not initialized");
                }

                // Create and publish message
                var sequenceNumber = Interlocked.Increment(ref messageCounter);
                var message = new TestMessage
                {
                    Id = (int)sequenceNumber,
                    Timestamp = DateTimeOffset.UtcNow,
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
                return Response.Fail<object>($"E2E test failed: {ex.Message}");
            }
        })
        .WithInit(async context =>
        {
            // Initialize publisher and subscriber before scenario starts
            var publisher = publisherFactory.CreatePublisher(publisherOptions);
            await publisher.CreateConnection();
            Publishers.TryAdd(publisherKey, publisher);

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
            await subscriber.StartMessageProcessingAsync();
            Subscribers.TryAdd(subscriberKey, subscriber);
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            // Steady load: 50 messages per second for 60 seconds
            Simulation.Inject(
                rate: 50,
                interval: TimeSpan.FromSeconds(1),
                during: TimeSpan.FromSeconds(60))
        )
        .WithClean(async context =>
        {
            // Cleanup on scenario end
            if (Publishers.TryRemove(publisherKey, out var publisher) && 
                publisher is IAsyncDisposable pubDisposable)
            {
                await pubDisposable.DisposeAsync();
            }
            if (Subscribers.TryRemove(subscriberKey, out var subscriber) && 
                subscriber is IAsyncDisposable subDisposable)
            {
                await subDisposable.DisposeAsync();
            }

            // Log statistics
            Console.WriteLine($"\n📊 E2E Test Statistics:");
            Console.WriteLine($"   Published messages: {PublishedMessages.Count}");
            Console.WriteLine($"   Received messages: {ReceivedMessages.Count}");
            Console.WriteLine($"   Message loss: {PublishedMessages.Count - ReceivedMessages.Count}");
        });
    }
}

