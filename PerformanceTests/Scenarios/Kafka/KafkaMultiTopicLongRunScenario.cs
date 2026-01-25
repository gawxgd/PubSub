using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Models;
using System.Text.Json;

namespace PerformanceTests.Scenarios;

public static class KafkaMultiTopicLongRunScenario
{
    // ----- per-run state -----
    private static readonly ConcurrentDictionary<string, TopicState> StateByTopic = new();
    private static readonly ConcurrentDictionary<string, List<IProducer<Null, TestMessage>>> ProducersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<IConsumer<Null, TestMessage>>> ConsumersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<Task>> ConsumerTasksByTopic = new();
    private static readonly ConcurrentDictionary<string, CancellationTokenSource> ConsumerCtsByTopic = new();

    // subscriber throughput CSV
    private static CancellationTokenSource? _csvCts;
    private static Task? _csvTask;

    private sealed class TopicState
    {
        public DateTime RunStartedUtc;
        public long PublishedOk;
        public long ReceivedTotal;
        public long ReceivedE2E;
        public long SkippedStale;
        public long ClockSkewed;
        public ReservoirSampler E2E;

        public TopicState(int maxSamples) => E2E = new ReservoirSampler(maxSamples);
    }

    private sealed class ReservoirSampler
    {
        private readonly int _max;
        private readonly double[] _buf;
        private int _filled;
        private long _seen;
        private readonly object _lock = new();
        private static readonly ThreadLocal<Random> _rng = new(() => new Random());

        public ReservoirSampler(int maxSamples)
        {
            _max = Math.Max(1, maxSamples);
            _buf = new double[_max];
        }

        public void Add(double value)
        {
            lock (_lock)
            {
                _seen++;
                if (_filled < _max)
                {
                    _buf[_filled++] = value;
                    return;
                }

                var r = _rng.Value!;
                var j = (long)(r.NextDouble() * _seen);
                if (j < _max)
                    _buf[(int)j] = value;
            }
        }

        public List<double> Snapshot()
        {
            lock (_lock)
                return _buf.Take(_filled).ToList();
        }

        public long SeenCount { get { lock (_lock) return _seen; } }
    }

    private static void ResetAll()
    {
        StateByTopic.Clear();
        ProducersByTopic.Clear();
        ConsumersByTopic.Clear();
        ConsumerTasksByTopic.Clear();

        foreach (var cts in ConsumerCtsByTopic.Values)
            try { cts.Cancel(); } catch { }
        ConsumerCtsByTopic.Clear();
    }

    public static void StartSubscriberThroughputCsv(string path)
    {
        _csvCts = new CancellationTokenSource();
        var token = _csvCts.Token;

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        var writer = new StreamWriter(stream, Encoding.UTF8, bufferSize: 64 * 1024);
        writer.WriteLine("ts_utc,topic,received_total,received_delta,msgs_per_sec");

        var last = new Dictionary<string, long>();

        _csvTask = Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var ts = DateTime.UtcNow;

                    foreach (var kvp in StateByTopic.ToArray())
                    {
                        var topic = kvp.Key;
                        var total = Interlocked.Read(ref kvp.Value.ReceivedE2E);

                        last.TryGetValue(topic, out var prev);
                        var delta = total - prev;
                        last[topic] = total;

                        await writer.WriteLineAsync($"{ts:O},{topic},{total},{delta},{delta}");
                    }

                    await writer.FlushAsync();
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                try { writer.Flush(); } catch { }
                try { writer.Dispose(); } catch { }
            }
        }, token);
    }

    public static async Task StopSubscriberThroughputCsvAsync()
    {
        if (_csvCts is null) return;
        _csvCts.Cancel();

        try { if (_csvTask != null) await _csvTask; } catch { }
        try { _csvCts.Dispose(); } catch { }

        _csvCts = null;
        _csvTask = null;
    }

    private static async Task EnsureTopicAsync(string bootstrapServers, string topicName, short replicationFactor = 1, int partitions = 1)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = replicationFactor
                }
            });
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // ok
        }
    }

    private static async Task DeleteTopicAsync(string bootstrapServers, string topicName)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        try { await admin.DeleteTopicsAsync(new[] { topicName }); }
        catch { /* optional */ }
    }

    public static ScenarioProps[] Create(
        string bootstrapServers,
        int numTopics = 10,
        int producersPerTopic = 1,
        int consumersPerTopic = 1,
        int totalRate = 5000,
        int durationSeconds = 60 * 60 * 2,
        int drainSeconds = 10,
        int maxE2ESamplesPerTopic = 200_000,
        bool deleteTopicsOnClean = false)
    {
        ResetAll();

        var ratePerTopic = Math.Max(1, totalRate / Math.Max(1, numTopics));
        var staleTolerance = TimeSpan.FromMinutes(5);

        var scenarios = new List<ScenarioProps>();

        for (int topicIndex = 1; topicIndex <= numTopics; topicIndex++)
        {
            var topicName = $"kafka-e2e-topic-{topicIndex:D2}";
            long messageCounter = 0L;

            // --- PUB scenario ---
            var pub = Scenario.Create($"pub_{topicName}", async _ =>
            {
                if (!ProducersByTopic.TryGetValue(topicName, out var producers) || producers.Count == 0)
                    return Response.Fail<object>("No producers");

                var idx = (int)(Interlocked.Read(ref messageCounter) % producers.Count);
                var producer = producers[idx];

                var seq = Interlocked.Increment(ref messageCounter);
                var publishedAtUtc = DateTime.UtcNow;

                var message = new TestMessage
                {
                    Id = (int)seq,
                    Timestamp = publishedAtUtc.Ticks, // Ticks UTC jak u Ciebie
                    Content = $"Kafka E2E {topicName} #{seq}",
                    Source = topicName,
                    SequenceNumber = seq
                };

                producer.Produce(topicName, new Message<Null, TestMessage> { Value = message });

                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));
                Interlocked.Increment(ref st.PublishedOk);

                return Response.Ok();
            })
            .WithInit(async _ =>
            {
                await EnsureTopicAsync(bootstrapServers, topicName);

                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    Acks = Acks.Leader,
                    EnableIdempotence = false,

                    // batching: tradeoff throughput/latency
                    LingerMs = 5,      // linger.ms -> batching 
                    BatchSize = 16384, // batch.size default ~16KB 

                    // safety defaults
                    MessageTimeoutMs = 120000,
                };

                var list = new List<IProducer<Null, TestMessage>>(producersPerTopic);
                for (int i = 0; i < producersPerTopic; i++)
                {
                    var p = new ProducerBuilder<Null, TestMessage>(producerConfig)
                        .SetValueSerializer(new KafkaJsonSerializer<TestMessage>())
                        .Build();
                    list.Add(p);
                }

                ProducersByTopic[topicName] = list;
            })
            .WithWarmUpDuration(TimeSpan.FromSeconds(10))
            .WithLoadSimulations(
                Simulation.Inject(rate: ratePerTopic, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(durationSeconds))
            )
            .WithClean(async _ =>
            {
                if (ProducersByTopic.TryRemove(topicName, out var ps))
                {
                    foreach (var p in ps)
                    {
                        try { p.Flush(TimeSpan.FromSeconds(5)); } catch { }
                        try { p.Dispose(); } catch { }
                    }
                }
            });

            scenarios.Add(pub);

            // --- SUB scenario (sonda NBomber + uruchomione consumer tasks) ---
            var sub = Scenario.Create($"sub_{topicName}", async ctx =>
            {
                try { await Task.Delay(1000, ctx.ScenarioCancellationToken); }
                catch (OperationCanceledException)
                {
                    return Response.Ok(statusCode: "STOP", message: "Stopping", sizeBytes: 0);
                }

                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));
                var total = Interlocked.Read(ref st.ReceivedTotal);
                var e2e = Interlocked.Read(ref st.ReceivedE2E);
                var stale = Interlocked.Read(ref st.SkippedStale);
                var skew = Interlocked.Read(ref st.ClockSkewed);

                return Response.Ok(statusCode: "ALIVE", message: $"recv_total={total} e2e={e2e} stale={stale} skew={skew}", sizeBytes: 0);
            })
            .WithInit(async _ =>
            {
                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));
                st.RunStartedUtc = DateTime.UtcNow;

                var cts = new CancellationTokenSource();
                ConsumerCtsByTopic[topicName] = cts;
                ConsumerTasksByTopic[topicName] = new List<Task>();

                var consumers = new List<IConsumer<Null, TestMessage>>(consumersPerTopic);

                for (int i = 0; i < consumersPerTopic; i++)
                {
                    // Fan-out: unikalna grupa -> każdy consumer dostaje ALL messages [web:223]
                    var groupId = $"e2e-{topicName}-{i}-{Guid.NewGuid():N}";

                    var consumerConfig = new ConsumerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        GroupId = groupId,
                        AutoOffsetReset = AutoOffsetReset.Latest, // działa “od teraz” dla nowej grupy [web:223]
                        EnableAutoCommit = true,
                        FetchWaitMaxMs = 25
                    };

                    var consumer = new ConsumerBuilder<Null, TestMessage>(consumerConfig)
                        .SetValueDeserializer(new KafkaJsonDeserializer<TestMessage>())
                        .Build();

                    consumer.Subscribe(topicName);
                    consumers.Add(consumer);

                    var task = Task.Run(() =>
                    {
                        try
                        {
                            while (!cts.IsCancellationRequested)
                            {
                                var cr = consumer.Consume(TimeSpan.FromMilliseconds(250));
                                if (cr?.Message?.Value == null) continue;

                                var msg = cr.Message.Value;
                                var receivedAt = DateTime.UtcNow;

                                Interlocked.Increment(ref st.ReceivedTotal);

                                var publishedAt = new DateTime(msg.Timestamp, DateTimeKind.Utc);

                                if (publishedAt < st.RunStartedUtc - staleTolerance)
                                {
                                    Interlocked.Increment(ref st.SkippedStale);
                                    continue;
                                }

                                var e2eMs = (receivedAt - publishedAt).TotalMilliseconds;
                                if (e2eMs < 0)
                                {
                                    Interlocked.Increment(ref st.ClockSkewed);
                                    e2eMs = 0;
                                }

                                st.E2E.Add(e2eMs);
                                Interlocked.Increment(ref st.ReceivedE2E);
                            }
                        }
                        catch { /* ignore */ }
                        finally
                        {
                            try { consumer.Close(); } catch { }
                            try { consumer.Dispose(); } catch { }
                        }
                    }, cts.Token);

                    ConsumerTasksByTopic[topicName].Add(task);
                }

                ConsumersByTopic[topicName] = consumers;
            })
            .WithWarmUpDuration(TimeSpan.FromSeconds(10))
            .WithLoadSimulations(
                Simulation.KeepConstant(copies: 1, during: TimeSpan.FromSeconds(durationSeconds + drainSeconds))
            )
            .WithClean(async _ =>
            {
                // stop consumers
                if (ConsumerCtsByTopic.TryRemove(topicName, out var cts))
                {
                    try { cts.Cancel(); } catch { }
                    try { cts.Dispose(); } catch { }
                }

                if (ConsumerTasksByTopic.TryRemove(topicName, out var tasks))
                    await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(3000));

                // snapshot + percentile file
                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));
                var samples = st.E2E.Snapshot();
                samples.Sort();

                if (samples.Count > 0)
                {
                    double P(double q)
                    {
                        var idx = (int)Math.Round(q * (samples.Count - 1));
                        idx = Math.Clamp(idx, 0, samples.Count - 1);
                        return samples[idx];
                    }

                    var reportsDir = Path.Combine(Directory.GetCurrentDirectory(), "reports");
                    Directory.CreateDirectory(reportsDir);

                    var content =
                        $"E2E Latency: {topicName}\n" +
                        $"Samples: {samples.Count:N0}\n" +
                        $"P50: {P(0.50):F2}ms\n" +
                        $"P95: {P(0.95):F2}ms\n" +
                        $"P99: {P(0.99):F2}ms\n";

                    File.WriteAllText(Path.Combine(reportsDir, $"e2e_{topicName}_{DateTime.Now:yyyyMMdd_HHmmss}.txt"), content);
                }

                if (deleteTopicsOnClean)
                    await DeleteTopicAsync(bootstrapServers, topicName);
            });

            scenarios.Add(sub);
        }

        return scenarios.ToArray();
    }

    public static void SaveGlobalE2EToFile()
    {
        var all = StateByTopic.Values.SelectMany(s => s.E2E.Snapshot()).ToList();
        if (all.Count == 0) return;

        all.Sort();

        double P(double q)
        {
            var idx = (int)Math.Round(q * (all.Count - 1));
            idx = Math.Clamp(idx, 0, all.Count - 1);
            return all[idx];
        }

        var reportsDir = Path.Combine(Directory.GetCurrentDirectory(), "reports");
        Directory.CreateDirectory(reportsDir);

        var txt =
            $"E2E Latency: GLOBAL\n" +
            $"Samples: {all.Count:N0}\n" +
            $"P50: {P(0.50):F2}ms\n" +
            $"P75: {P(0.75):F2}ms\n" +
            $"P95: {P(0.95):F2}ms\n" +
            $"P99: {P(0.99):F2}ms\n";

        File.WriteAllText(Path.Combine(reportsDir, $"e2e_global_{DateTime.Now:yyyyMMdd_HHmmss}.txt"), txt);
    }


    public sealed class KafkaJsonSerializer<T> : ISerializer<T>
    {
        private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

        public byte[] Serialize(T data, SerializationContext context)
            => data is null ? Array.Empty<byte>() : JsonSerializer.SerializeToUtf8Bytes(data, Options);
    }

    public sealed class KafkaJsonDeserializer<T> : IDeserializer<T>
    {
        private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.IsEmpty) return default!;
            return JsonSerializer.Deserialize<T>(data, Options)!;
        }
    }
}

