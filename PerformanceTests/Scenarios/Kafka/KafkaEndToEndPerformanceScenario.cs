using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Infrastructure;
using PerformanceTests.Models;

namespace PerformanceTests.Scenarios;

/// <summary>
/// End-to-end performance test for Kafka: publish messages and verify they are received by consumer.
/// </summary>
public static class KafkaEndToEndPerformanceScenario
{
    private static readonly ConcurrentDictionary<long, DateTime> PublishedMessages = new();
    private static readonly ConcurrentDictionary<long, DateTime> ReceivedMessages = new();
    private static readonly ConcurrentDictionary<string, IProducer<Null, TestMessage>> Producers = new();
    private static readonly ConcurrentDictionary<string, IConsumer<Null, TestMessage>> Consumers = new();
    private static readonly ConcurrentDictionary<string, Task> ConsumerTasks = new();
    private static CancellationTokenSource? _consumerCancellationSource;

    public static ScenarioProps Create(string bootstrapServers, string schemaRegistryUrl, string topic, string consumerGroupId)
    {
        var messageCounter = 0L;
        const string producerKey = "kafka_e2e_publisher";
        const string consumerKey = "kafka_e2e_consumer";

        return Scenario.Create("kafka_end_to_end_throughput", async context =>
        {
            try
            {
                if (!Producers.TryGetValue(producerKey, out var producer))
                {
                    Console.WriteLine($"ERROR: Kafka Producer '{producerKey}' not found");
                    return Response.Fail<object>("Kafka Producer not initialized");
                }

                var sequenceNumber = Interlocked.Increment(ref messageCounter);
                var message = new TestMessage
                {
                    Id = (int)sequenceNumber,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = $"Kafka E2E test message #{sequenceNumber}",
                    SequenceNumber = sequenceNumber
                };

                var publishedAt = DateTime.UtcNow;
                PublishedMessages.TryAdd(sequenceNumber, publishedAt);

                var kafkaMessage = new Message<Null, TestMessage> { Value = message };
                
                try
                {
                    var deliveryResult = await producer.ProduceAsync(topic, kafkaMessage);
                    
                    // Check if delivery was successful
                    if (deliveryResult.Status == PersistenceStatus.Persisted || 
                        deliveryResult.Status == PersistenceStatus.PossiblyPersisted)
                    {
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
                var errorMsg = $"Kafka E2E test failed: {ex.GetType().Name} - {ex.Message}";
                if (ex.InnerException != null)
                {
                    errorMsg += $" (Inner: {ex.InnerException.GetType().Name} - {ex.InnerException.Message})";
                }
                Console.WriteLine($"ERROR in kafka_end_to_end_throughput: {errorMsg}");
                return Response.Fail<object>(errorMsg);
            }
        })
        .WithInit(async context =>
        {
            try
            {
                Console.WriteLine($"Initializing Kafka E2E scenario: producer and consumer");

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
                Console.WriteLine($" Kafka Producer '{producerKey}' initialized");

                // Create Kafka consumer
                var consumerBootstrapServers = bootstrapServers.Contains("localhost") 
                    ? bootstrapServers.Replace("localhost", "127.0.0.1")
                    : bootstrapServers;
                    
                // Use unique group ID with timestamp to ensure fresh start each test run
                var uniqueGroupId = $"{consumerGroupId}-{DateTime.UtcNow:yyyyMMddHHmmss}";
                    
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = consumerBootstrapServers,
                    GroupId = uniqueGroupId, // Unique group ID to start fresh each time
                    AutoOffsetReset = AutoOffsetReset.Earliest, // Start from beginning
                    EnableAutoCommit = true,
                    EnablePartitionEof = true,
                    SocketKeepaliveEnable = true,
                    SocketTimeoutMs = 60000,
                    AllowAutoCreateTopics = true, // Allow topic creation if it doesn't exist
                    SessionTimeoutMs = 10000,
                    MaxPollIntervalMs = 300000,
                    FetchMinBytes = 1, // Don't wait for multiple messages
                    FetchWaitMaxMs = 100 // Max wait time for fetch
                };

                var consumer = new ConsumerBuilder<Null, TestMessage>(consumerConfig)
                    .SetValueDeserializer(new KafkaJsonDeserializer<TestMessage>())
                    .Build();

                consumer.Subscribe(topic);
                Console.WriteLine($"Kafka Consumer '{consumerKey}' subscribed to topic '{topic}'");

                // Start consumer loop in background
                _consumerCancellationSource = new CancellationTokenSource();
                var messagesReceivedCount = 0L;
                var messageProcessingTask = Task.Run(() =>
                {
                    try
                    {
                        Console.WriteLine($"Kafka Consumer loop started for topic '{topic}'");
                        while (!_consumerCancellationSource.Token.IsCancellationRequested)
                        {
                            try
                            {
                                // Use timeout to allow cancellation check (100ms timeout)
                                var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                                
                                if (result == null)
                                {
                                    // Timeout - no message available, continue polling
                                    continue;
                                }
                                
                                if (result.IsPartitionEOF)
                                {
                                    // End of partition - continue polling
                                    continue;
                                }
                                
                                if (result.Message == null)
                                {
                                    // No message - continue polling
                                    continue;
                                }
                                
                                // Try to deserialize message
                                if (result.Message.Value == null)
                                {
                                    Console.WriteLine($"⚠️  Warning: Received message with null Value (Key: {result.Message.Key}, Offset: {result.Offset})");
                                    continue;
                                }
                                
                                var message = result.Message.Value;
                                var receivedAt = DateTime.UtcNow;
                                var count = Interlocked.Increment(ref messagesReceivedCount);
                                
                                if (count <= 5 || count % 50 == 0)
                                {
                                    Console.WriteLine($"📥 Kafka Consumer received message #{count}: SequenceNumber={message.SequenceNumber}, Id={message.Id}");
                                }
                                
                                ReceivedMessages.TryAdd(message.SequenceNumber, receivedAt);

                                if (PublishedMessages.TryGetValue(message.SequenceNumber, out var publishedAt))
                                {
                                    var latency = receivedAt - publishedAt;
                                    // Latency is tracked but not logged to avoid spam
                                }
                                else
                                {
                                    Console.WriteLine($"⚠️  Warning: Received message with SequenceNumber {message.SequenceNumber} that was not published");
                                }
                            }
                            catch (ConsumeException ex)
                            {
                                // Check if it's a partition EOF (normal case) - EOF is indicated by IsPartitionEOF, not error code
                                if (!ex.Error.IsFatal)
                                {
                                    // Non-fatal error, log and continue polling
                                    if (ex.Error.Code != ErrorCode.Local_ValueDeserialization)
                                    {
                                        Console.WriteLine($"⚠️  Non-fatal Kafka consumer error: {ex.Error.Code} - {ex.Error.Reason}");
                                    }
                                    continue;
                                }
                                else
                                {
                                    Console.WriteLine($"⚠️  Fatal Kafka consumer error: {ex.Error.Code} - {ex.Error.Reason}");
                                    break;
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"⚠️  Exception in consumer loop: {ex.GetType().Name} - {ex.Message}");
                                if (ex.InnerException != null)
                                {
                                    Console.WriteLine($"   Inner: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                                }
                                // Continue polling despite error
                                continue;
                            }
                        }
                        Console.WriteLine($"Kafka Consumer loop ended. Total messages received: {messagesReceivedCount}");
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine($"Kafka Consumer loop cancelled. Total messages received: {messagesReceivedCount}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Fatal error in Kafka consumer loop: {ex.GetType().Name} - {ex.Message}");
                        if (ex.InnerException != null)
                        {
                            Console.WriteLine($"   Inner: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                        }
                    }
                });
                
                Consumers.TryAdd(consumerKey, consumer);
                ConsumerTasks.TryAdd(consumerKey, messageProcessingTask);
                Console.WriteLine($"Kafka E2E scenario initialized successfully");
                
                // Give consumer time to start and join consumer group
                await Task.Delay(TimeSpan.FromSeconds(2));
                Console.WriteLine($"Kafka Consumer ready, starting test...");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR initializing Kafka E2E scenario: {ex.GetType().Name} - {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner exception: {ex.InnerException.GetType().Name} - {ex.InnerException.Message}");
                }
                throw;
            }
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(3))
        .WithLoadSimulations(
            // Steady load: 20 messages per second for 20 seconds (matching PubSub test)
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
                _consumerCancellationSource?.Cancel();
                
                // Wait for consumer task to finish (with timeout)
                if (ConsumerTasks.TryRemove(consumerKey, out var consumerTask))
                {
                    try
                    {
                        await consumerTask.WaitAsync(TimeSpan.FromSeconds(2));
                    }
                    catch (TimeoutException)
                    {
                        Console.WriteLine("Warning: Consumer task did not finish within timeout");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Warning: Error waiting for consumer task: {ex.Message}");
                    }
                }
                
                if (Consumers.TryRemove(consumerKey, out var consumer))
                {
                    try
                    {
                        consumer.Close();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Warning: Error closing Kafka consumer: {ex.Message}");
                    }
                    finally
                    {
                        try
                        {
                            consumer.Dispose();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Warning: Error disposing Kafka consumer: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Error in Kafka consumer cleanup: {ex.Message}");
            }
            
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
            

            Console.WriteLine($"\n Kafka E2E Test Statistics:");
            Console.WriteLine($"   Published messages: {PublishedMessages.Count}");
            Console.WriteLine($"   Received messages: {ReceivedMessages.Count}");
            Console.WriteLine($"   Message loss: {PublishedMessages.Count - ReceivedMessages.Count}");
        });
    }
}

