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
                    Console.WriteLine($"ERROR: Publisher '{publisherKey}' not found in dictionary");
                    return Response.Fail<object>("Publisher not initialized");
                }

                // Create test message
                var message = new TestMessage
                {
                    Id = (int)Interlocked.Increment(ref messageCounter),
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = $"Performance test message #{messageCounter}",
                    SequenceNumber = messageCounter
                };

                // Publish message
                await publisher.PublishAsync(message);

                return Response.Ok();
            }
            catch (Exception ex)
            {
                var errorMsg = $"Publish failed: {ex.GetType().Name} - {ex.Message}";
                if (ex.InnerException != null)
                {
                    errorMsg += $" (Inner: {ex.InnerException.GetType().Name} - {ex.InnerException.Message})";
                }
                Console.WriteLine($" ERROR in publisher_throughput: {errorMsg}");
                return Response.Fail<object>(errorMsg);
            }
        })
        .WithInit(async context =>
        {
            try
            {
                Console.WriteLine($"Initializing publisher for scenario: {publisherKey}");
                // Initialize publisher before scenario starts
                var publisher = publisherFactory.CreatePublisher(options);
                await publisher.CreateConnection();
                
                // Give publisher time to register schema and be ready
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                
                Publishers.TryAdd(publisherKey, publisher);
                Console.WriteLine($"Publisher '{publisherKey}' initialized successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR initializing publisher '{publisherKey}': {ex.GetType().Name} - {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner exception: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                }
                throw;
            }
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(3))
        .WithLoadSimulations(
            // Steady load: 10 messages per second for 15 seconds (reduced for faster tests)
            Simulation.Inject(
                rate: 10,
                interval: TimeSpan.FromSeconds(1),
                during: TimeSpan.FromSeconds(15))
        )
        .WithClean(async context =>
        {
            // Cleanup on scenario end with timeout
            try
            {
                if (Publishers.TryRemove(publisherKey, out var publisher) && 
                    publisher is IAsyncDisposable disposable)
                {
                    var disposeTask = disposable.DisposeAsync().AsTask();
                    var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
                    var completedTask = await Task.WhenAny(disposeTask, timeoutTask);
                    if (completedTask == timeoutTask)
                    {
                        Console.WriteLine("Warning: Publisher disposal timed out");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Error disposing publisher: {ex.Message}");
            }
        });
    }
}

