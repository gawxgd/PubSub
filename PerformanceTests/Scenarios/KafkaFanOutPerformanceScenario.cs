using System.Collections.Concurrent;
using Confluent.Kafka;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Infrastructure;
using PerformanceTests.Models;

namespace PerformanceTests.Scenarios;

/// <summary>
/// Kafka fan-out performance test:
/// one producer, multiple independent consumer groups on the same topic.
/// </summary>
public static class KafkaFanOutPerformanceScenario
{
    private static readonly ConcurrentDictionary<long, DateTime> PublishedMessages = new();

    // consumerId -> (sequenceNumber -> receivedAt)
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DateTime>> ReceivedMessages
        = new();

    private static readonly ConcurrentDictionary<string, IConsumer<Null, TestMessage>> Consumers = new();
    private static readonly ConcurrentDictionary<string, Task> ConsumerTasks = new();

    private static IProducer<Null, TestMessage>? Producer;
    private static CancellationTokenSource? _cts;

    public static ScenarioProps Create(
        int consumerCount,
        string bootstrapServers,
        string topic,
        string consumerGroupPrefix)
    {
        var messageCounter = 0L;

        return Scenario.Create($"kafka_fanout_{consumerCount}_consumers", async context =>
        {
            try
            {
                if (Producer == null)
                {
                    return Response.Fail<object>("Kafka Producer not initialized");
                }

                var sequenceNumber = Interlocked.Increment(ref messageCounter);

                var message = new TestMessage
                {
                    Id = (int)sequenceNumber,
                    SequenceNumber = sequenceNumber,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Content = $"Kafka fan-out message #{sequenceNumber}"
                };

                PublishedMessages.TryAdd(sequenceNumber, DateTime.UtcNow);

                var result = await Producer.ProduceAsync(
                    topic,
                    new Message<Null, TestMessage> { Value = message });

                return result.Status == PersistenceStatus.Persisted
                    ? Response.Ok()
                    : Response.Fail<object>($"Produce failed: {result.Status}");
            }
            catch (Exception ex)
            {
                return Response.Fail<object>(
                    $"Kafka fan-out publish error: {ex.GetType().Name} - {ex.Message}");
            }
        })
        .WithInit(async context =>
        {
            Console.WriteLine(
                $"Initializing Kafka fan-out scenario with {consumerCount} consumer groups");

            _cts = new CancellationTokenSource();

            // --- Producer ---
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1"),
                Acks = Acks.All,
                EnableIdempotence = true
            };

            Producer = new ProducerBuilder<Null, TestMessage>(producerConfig)
                .SetValueSerializer(new KafkaJsonSerializer<TestMessage>())
                .Build();

            Console.WriteLine("Kafka Producer initialized");

            // --- Consumers (fan-out via multiple consumer groups) ---
            for (int i = 0; i < consumerCount; i++)
            {
                var consumerId = $"consumer-{i}";
                var groupId = $"{consumerGroupPrefix}-{i}-{DateTime.UtcNow:HHmmss}";

                var receivedMap = new ConcurrentDictionary<long, DateTime>();
                ReceivedMessages.TryAdd(consumerId, receivedMap);

                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers.Replace("localhost", "127.0.0.1"),
                    GroupId = groupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true,
                    FetchWaitMaxMs = 100
                };

                var consumer = new ConsumerBuilder<Null, TestMessage>(consumerConfig)
                    .SetValueDeserializer(new KafkaJsonDeserializer<TestMessage>())
                    .Build();

                consumer.Subscribe(topic);

                var task = Task.Run(() =>
                {
                    try
                    {
                        while (!_cts.Token.IsCancellationRequested)
                        {
                            var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
                            if (cr?.Message?.Value == null || cr.IsPartitionEOF)
                                continue;

                            receivedMap.TryAdd(
                                cr.Message.Value.SequenceNumber,
                                DateTime.UtcNow);
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        Console.WriteLine(
                            $"⚠️  Consumer {consumerId} error: {ex.Message}");
                    }
                });

                Consumers.TryAdd(consumerId, consumer);
                ConsumerTasks.TryAdd(consumerId, task);

                Console.WriteLine(
                    $"Kafka Consumer '{consumerId}' started (groupId={groupId})");
            }

            await Task.Delay(TimeSpan.FromSeconds(2));
            Console.WriteLine("Kafka fan-out scenario initialized");
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
            Console.WriteLine("\nKafka Fan-out Test Statistics:");

            foreach (var (consumerId, received) in ReceivedMessages)
            {
                var lost = PublishedMessages.Count - received.Count;
                Console.WriteLine(
                    $"  {consumerId}: received={received.Count}, lost={lost}");
            }

            _cts?.Cancel();

            foreach (var (id, task) in ConsumerTasks)
            {
                try { await task.WaitAsync(TimeSpan.FromSeconds(2)); }
                catch { }
            }

            foreach (var consumer in Consumers.Values)
            {
                try
                {
                    consumer.Close();
                    consumer.Dispose();
                }
                catch { }
            }

            Consumers.Clear();
            ConsumerTasks.Clear();
            ReceivedMessages.Clear();

            if (Producer != null)
            {
                Producer.Flush(TimeSpan.FromSeconds(10));
                Producer.Dispose();
                Producer = null;
            }

            PublishedMessages.Clear();
        });
    }
}
