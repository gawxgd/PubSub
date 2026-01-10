using System.Collections.Concurrent;
using Confluent.Kafka;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Infrastructure;
using PerformanceTests.Models;

namespace PerformanceTests.Scenarios;

/// <summary>
/// Latency test for Kafka publisher - measures how long it takes to publish a single message to Kafka.
/// </summary>
public static class KafkaPublisherLatencyScenario
{
    private static readonly ConcurrentDictionary<string, IProducer<Null, TestMessage>> Producers = new();

    public static ScenarioProps Create(string bootstrapServers, string schemaRegistryUrl, string topic)
    {
        var messageCounter = 0L;
        const string producerKey = "kafka_publisher_latency";

        return Scenario.Create("kafka_publisher_latency", async context =>
        {
            try
            {
                if (!Producers.TryGetValue(producerKey, out var producer))
                {
                    Console.WriteLine($"ERROR: Kafka Producer '{producerKey}' not found");
                    return Response.Fail<object>("Kafka Producer not initialized");
                }

                // Create test message
                var message = new TestMessage
                {
                    Id = (int)Interlocked.Increment(ref messageCounter),
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = $"Kafka latency test message #{messageCounter}",
                    SequenceNumber = messageCounter
                };

                // Measure publish latency
                var kafkaMessage = new Message<Null, TestMessage> { Value = message };
                
                try
                {
                    var deliveryResult = await producer.ProduceAsync(topic, kafkaMessage);
                    
                    // Check if delivery was successful
                    if (deliveryResult.Status == PersistenceStatus.Persisted || 
                        deliveryResult.Status == PersistenceStatus.PossiblyPersisted)
                    {
                        // NBomber will track latency automatically from the response time
                        return Response.Ok();
                    }
                    else
                    {
                        return Response.Fail<object>($"Message delivery failed with status: {deliveryResult.Status}");
                    }
                }
                catch (ProduceException<Null, TestMessage> ex)
                {
                    var errorMsg = $"ProduceException: {ex.Error.Reason} (Code: {ex.Error.Code})";
                    if (ex.InnerException != null)
                    {
                        errorMsg += $", Inner: {ex.InnerException.Message}";
                    }
                    return Response.Fail<object>(errorMsg);
                }
            }
            catch (Exception ex)
            {
                var errorMsg = $"Kafka publish failed: {ex.GetType().Name} - {ex.Message}";
                if (ex.InnerException != null)
                {
                    errorMsg += $" (Inner: {ex.InnerException.GetType().Name} - {ex.InnerException.Message})";
                }
                Console.WriteLine($"ERROR in kafka_publisher_latency: {errorMsg}");
                return Response.Fail<object>(errorMsg);
            }
        })
        .WithInit(async context =>
        {
            try
            {
                Console.WriteLine($"Initializing Kafka producer for scenario: {producerKey}");

                // Create Kafka producer with JSON serializer (no Schema Registry needed for JSON)
                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    Acks = Acks.All,
                    EnableIdempotence = true,
                    MaxInFlight = 5,
                    RetryBackoffMs = 100,
                    SocketKeepaliveEnable = true,
                    SocketTimeoutMs = 60000
                };
                
                // Replace localhost with 127.0.0.1 to force IPv4
                if (bootstrapServers.Contains("localhost"))
                {
                    producerConfig.BootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1");
                }

                var producer = new ProducerBuilder<Null, TestMessage>(producerConfig)
                    .SetValueSerializer(new KafkaJsonSerializer<TestMessage>())
                    .Build();

                Producers.TryAdd(producerKey, producer);
                Console.WriteLine($"Kafka Producer '{producerKey}' initialized successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR initializing Kafka producer '{producerKey}': {ex.GetType().Name} - {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner exception: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                }
                throw;
            }
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(3))
        .WithLoadSimulations(
            // Constant rate: 10 messages per second for 15 seconds (matching PubSub test)
            // This allows us to measure latency under steady load
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
                if (Producers.TryRemove(producerKey, out var producer))
                {
                    // Flush all pending messages before disposing
                    producer.Flush(TimeSpan.FromSeconds(10));
                    producer.Dispose();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Error disposing Kafka producer: {ex.Message}");
            }
            
        });
    }
}

