using System.Collections.Concurrent;
using NBomber.CSharp;
using PerformanceTests.Models;
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using NBomber.Contracts;

namespace PerformanceTests.Scenarios;

/// <summary>
/// Performance test scenario for publisher throughput.
/// Tests how many messages per second can be published.
/// </summary>
public static class PublisherPerformanceScenario
{
    private static readonly ConcurrentDictionary<string, IPublisher<TestMessage>> Publishers = new();

    public static ScenarioProps Create(IPublisherFactory<TestMessage> publisherFactory, PublisherOptions options)
    {
        var messageCounter = 0L;
        const string publisherKey = "publisher_throughput";

        return Scenario.Create("publisher_throughput", async context =>
        {
            try
            {
                // Get publisher instance (thread-safe)
                if (!Publishers.TryGetValue(publisherKey, out var publisher))
                {
                    return Response.Fail<object>("Publisher not initialized");
                }

                // Create test message
                var message = new TestMessage
                {
                    Id = (int)Interlocked.Increment(ref messageCounter),
                    Timestamp = DateTimeOffset.UtcNow,
                    Content = $"Performance test message #{messageCounter}",
                    SequenceNumber = messageCounter
                };

                // Publish message
                await publisher.PublishAsync(message);

                return Response.Ok();
            }
            catch (Exception ex)
            {
                return Response.Fail<object>($"Publish failed: {ex.Message}");
            }
        })
        .WithInit(async context =>
        {
            // Initialize publisher before scenario starts
            var publisher = publisherFactory.CreatePublisher(options);
            await publisher.CreateConnection();
            Publishers.TryAdd(publisherKey, publisher);
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            // Steady load: 10 messages per second for 30 seconds
            Simulation.Inject(
                rate: 10,
                interval: TimeSpan.FromSeconds(1),
                during: TimeSpan.FromSeconds(30))
        )
        .WithClean(async context =>
        {
            // Cleanup on scenario end
            if (Publishers.TryRemove(publisherKey, out var publisher) && 
                publisher is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync();
            }
        });
    }
}

