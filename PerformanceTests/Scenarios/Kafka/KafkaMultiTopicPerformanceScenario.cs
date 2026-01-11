using System.Collections.Concurrent;
using Confluent.Kafka;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Infrastructure;
using PerformanceTests.Models;

namespace PerformanceTests.Scenarios;

/// <summary>
/// Long-run multi-topic test for Kafka (simple, non-distributed setup like PubSub):
/// - 10 topics, each with 1 partition (no distribution)
/// - 10 producers per topic (independent)
/// - 5 consumers per topic (each gets ALL messages, not load-balanced)
/// Total: 5000 msg/s for 2 hours
/// </summary>
public static class KafkaMultiTopicLongRunScenario
{
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> PublishedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> ReceivedMessagesByTopic = new();
    private static readonly ConcurrentDictionary<string, List<IProducer<Null, TestMessage>>> ProducersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<IConsumer<Null, TestMessage>>> ConsumersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<Task>> ConsumerTasksByTopic = new();
    private static readonly ConcurrentDictionary<string, CancellationTokenSource> CancellationTokensByTopic = new();

    public static ScenarioProps[] Create(string bootstrapServers, string topicPrefix = "kafka-long-run-topic")
    {
        const int numTopics = 10;
        const int publishersPerTopic = 10;
        const int subscribersPerTopic = 5;
        const int totalRate = 5000; // msg/s total
        const int ratePerTopic = totalRate / numTopics; // 500 msg/s per topic
        const int durationSeconds = 2 * 60 * 60; // 2 hours

        var scenarios = new List<ScenarioProps>();

        // Fix localhost to IPv4
        var fixedBootstrapServers = bootstrapServers.Contains("localhost") 
            ? bootstrapServers.Replace("localhost", "127.0.0.1")
            : bootstrapServers;

        for (int topicIndex = 1; topicIndex <= numTopics; topicIndex++)
        {
            var topicName = $"{topicPrefix}-{topicIndex:D2}";
            var messageCounter = 0L;

            var scenario = Scenario.Create($"kafka_long_run_{topicName}", async context =>
            {
                try
                {
                    if (!ProducersByTopic.TryGetValue(topicName, out var producers) || producers.Count == 0)
                    {
                        Console.WriteLine($"ERROR: No Kafka producers found for topic '{topicName}'");
                        return Response.Fail<object>("Kafka producers not initialized");
                    }

                    // Round-robin producer selection
                    var producerIndex = (int)(Interlocked.Read(ref messageCounter) % producers.Count);
                    var producer = producers[producerIndex];

                    var sequenceNumber = Interlocked.Increment(ref messageCounter);
                    var message = new TestMessage
                    {
                        Id = (int)sequenceNumber,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Content = $"Kafka long-run test {topicName} #{sequenceNumber}",
                        Source = topicName,
                        SequenceNumber = sequenceNumber
                    };

                    var publishedAt = DateTime.UtcNow;
                    var publishedMessages = PublishedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());
                    publishedMessages.TryAdd(sequenceNumber, publishedAt);

                    var kafkaMessage = new Message<Null, TestMessage> { Value = message };
                    var deliveryResult = await producer.ProduceAsync(topicName, kafkaMessage);

                    if (deliveryResult.Status == PersistenceStatus.Persisted || 
                        deliveryResult.Status == PersistenceStatus.PossiblyPersisted)
                    {
                        return Response.Ok();
                    }
                    else
                    {
                        return Response.Fail<object>($"Message delivery failed: {deliveryResult.Status}");
                    }
                }
                catch (ProduceException<Null, TestMessage> ex)
                {
                    return Response.Fail<object>($"ProduceException: {ex.Error.Code} - {ex.Error.Reason}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR in {topicName}: {ex.Message}");
                    return Response.Fail<object>($"Kafka test failed: {ex.GetType().Name}");
                }
            })
            .WithInit(async context =>
            {
                try
                {
                    Console.WriteLine($"Initializing Kafka topic: {topicName}");
                    Console.WriteLine($"  Publishers: {publishersPerTopic}");
                    Console.WriteLine($"  Subscribers: {subscribersPerTopic} (each gets ALL messages)");
                    Console.WriteLine($"  Rate: {ratePerTopic} msg/s");
                    Console.WriteLine($"  Partitions: 1 (single partition, like PubSub)");

                    var producers = new List<IProducer<Null, TestMessage>>();
                    
                    // Producer config - similar to PubSub (strong guarantees)
                    // Producer config - matching your PubSub configuration
                    var producerConfig = new ProducerConfig
                    {
                        BootstrapServers = fixedBootstrapServers,
                        Acks = Acks.All, // Matching MaxSendAttempts/strong guarantees
                        EnableIdempotence = true,
                        MaxInFlight = 5,
                        RetryBackoffMs = 100,
                        SocketKeepaliveEnable = true,
                        SocketTimeoutMs = 60000,
                        
                        // Batching - matching your BatchMaxBytes and BatchMaxDelay
                        LingerMs = 100, // Your BatchMaxDelay = 100ms
                        BatchSize = 65536, // Your BatchMaxBytes = 64KB
                        CompressionType = CompressionType.None,
                        
                        // Retry config - matching MaxRetryAttempts = 3
                        MessageSendMaxRetries = 3,
                        
                        // Queue size - matching MaxPublisherQueueSize = 10000
                        QueueBufferingMaxMessages = 10000
                    };


                    for (int i = 0; i < publishersPerTopic; i++)
                    {
                        var producer = new ProducerBuilder<Null, TestMessage>(producerConfig)
                            .SetValueSerializer(new KafkaJsonSerializer<TestMessage>())
                            .Build();
                        producers.Add(producer);
                    }
                    ProducersByTopic.TryAdd(topicName, producers);
                    Console.WriteLine($"  ✓ {publishersPerTopic} Kafka producers initialized for {topicName}");

                    await Task.Delay(TimeSpan.FromMilliseconds(500));

                    // Create consumers - KAŻDY ma UNIKALNY GroupId = brak load-balancingu
                    // Każdy consumer dostaje WSZYSTKIE wiadomości (jak w twoim PubSub)
                    var consumers = new List<IConsumer<Null, TestMessage>>();
                    var consumerTasks = new List<Task>();
                    var cts = new CancellationTokenSource();
                    CancellationTokensByTopic.TryAdd(topicName, cts);

                    for (int i = 0; i < subscribersPerTopic; i++)
                    {
                        // KLUCZOWE: Każdy consumer ma UNIKALNY GroupId
                        // Dzięki temu NIE ma load-balancingu i każdy dostaje wszystkie wiadomości
                        var uniqueGroupId = $"kafka-consumer-{topicName}-{i}-{DateTime.UtcNow:yyyyMMddHHmmss}";

                        var consumerConfig = new ConsumerConfig
                        {
                            BootstrapServers = fixedBootstrapServers,
                            GroupId = uniqueGroupId, // UNIKALNY dla każdego consumera!
                            AutoOffsetReset = AutoOffsetReset.Latest,
                            EnableAutoCommit = true,
                            AutoCommitIntervalMs = 5000,
                            EnablePartitionEof = false,
                            SocketKeepaliveEnable = true,
                            SocketTimeoutMs = 60000,
                            SessionTimeoutMs = 45000,
                            MaxPollIntervalMs = 300000,
                            
                            // Polling - matching your PollInterval = 50ms
                            FetchMinBytes = 1, // Poll frequently (like your 50ms interval)
                            FetchWaitMaxMs = 50, // Your PollInterval = 50ms
                            MaxPartitionFetchBytes = 1048576
                        };


                        var consumer = new ConsumerBuilder<Null, TestMessage>(consumerConfig)
                            .SetValueDeserializer(new KafkaJsonDeserializer<TestMessage>())
                            .Build();

                        consumer.Subscribe(topicName);
                        consumers.Add(consumer);

                        var consumerIndex = i; // Capture for logging
                        
                        // Start consumer loop
                        var consumerTask = Task.Run(async () =>
                        {
                            try
                            {
                                while (!cts.Token.IsCancellationRequested)
                                {
                                    try
                                    {
                                        var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                                        
                                        if (result == null || result.IsPartitionEOF || result.Message?.Value == null)
                                            continue;

                                        var message = result.Message.Value;
                                        var receivedAt = DateTime.UtcNow;
                                        
                                        // Track per-consumer reception (optional, for debugging)
                                        var receivedMessages = ReceivedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());
                                        receivedMessages.TryAdd(message.SequenceNumber, receivedAt);

                                        var publishedMessages = PublishedMessagesByTopic.GetOrAdd(topicName, _ => new ConcurrentDictionary<long, DateTime>());
                                        if (publishedMessages.TryGetValue(message.SequenceNumber, out var publishedAt))
                                        {
                                            var latency = receivedAt - publishedAt;
                                            // Latency tracking
                                        }
                                    }
                                    catch (ConsumeException ex) when (!ex.Error.IsFatal)
                                    {
                                        // Non-fatal error, continue
                                        continue;
                                    }
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                // Expected during cleanup
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"⚠️ Consumer {consumerIndex} error in {topicName}: {ex.Message}");
                            }
                        });

                        consumerTasks.Add(consumerTask);
                    }

                    ConsumersByTopic.TryAdd(topicName, consumers);
                    ConsumerTasksByTopic.TryAdd(topicName, consumerTasks);
                    Console.WriteLine($"  ✓ {subscribersPerTopic} Kafka consumers initialized for {topicName}");
                    Console.WriteLine($"  ✓ Topic {topicName} ready\n");

                    // Give consumers time to connect and start polling
                    await Task.Delay(TimeSpan.FromSeconds(2));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR initializing Kafka topic {topicName}: {ex.GetType().Name} - {ex.Message}");
                    throw;
                }
            })
            .WithWarmUpDuration(TimeSpan.FromSeconds(10))
            .WithLoadSimulations(
                Simulation.Inject(
                    rate: ratePerTopic,
                    interval: TimeSpan.FromSeconds(1),
                    during: TimeSpan.FromSeconds(durationSeconds))
            )
            .WithClean(async context =>
            {
                var cleanupStart = DateTime.UtcNow;
                Console.WriteLine($"\n[{cleanupStart:HH:mm:ss}] === Kafka Cleanup START for {topicName} ===");

                // Stop consumers
                if (CancellationTokensByTopic.TryRemove(topicName, out var cts))
                {
                    cts.Cancel();
                }

                // Wait for consumer tasks to finish
                if (ConsumerTasksByTopic.TryRemove(topicName, out var consumerTasks))
                {
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Waiting for {consumerTasks.Count} consumer tasks...");
                    try
                    {
                        await Task.WhenAll(consumerTasks).WaitAsync(TimeSpan.FromSeconds(5));
                        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] ✓ All consumer tasks finished");
                    }
                    catch (TimeoutException)
                    {
                        Console.WriteLine($"⚠️ Consumer tasks timeout for {topicName}");
                    }
                }

                // Dispose consumers
                if (ConsumersByTopic.TryRemove(topicName, out var consumers))
                {
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Disposing {consumers.Count} Kafka consumers...");
                    for (int i = 0; i < consumers.Count; i++)
                    {
                        try
                        {
                            consumers[i].Close();
                            consumers[i].Dispose();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error disposing consumer {i}: {ex.Message}");
                        }
                    }
                }

                // Dispose producers
                if (ProducersByTopic.TryRemove(topicName, out var producers))
                {
                    Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Disposing {producers.Count} Kafka producers...");
                    for (int i = 0; i < producers.Count; i++)
                    {
                        try
                        {
                            producers[i].Flush(TimeSpan.FromSeconds(10));
                            producers[i].Dispose();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error disposing producer {i}: {ex.Message}");
                        }
                    }
                }

                // Statistics
                var publishedCount = PublishedMessagesByTopic.TryGetValue(topicName, out var pub) ? pub.Count : 0;
                var receivedCount = ReceivedMessagesByTopic.TryGetValue(topicName, out var rec) ? rec.Count : 0;
                
                var cleanupEnd = DateTime.UtcNow;
                var totalTime = (cleanupEnd - cleanupStart).TotalMinutes;
                Console.WriteLine($"[{cleanupEnd:HH:mm:ss}] === Kafka Cleanup END for {topicName} in {totalTime:F2} min ===");
                Console.WriteLine($"  Published: {publishedCount}, Received: {receivedCount}, Loss: {publishedCount - receivedCount}");
            });

            scenarios.Add(scenario);
        }

        return scenarios.ToArray();
    }
}
