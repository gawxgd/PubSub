using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NBomber.CSharp;
using NBomber.Contracts;
using PerformanceTests.Models;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;
using Subscriber.Configuration.Options;
using Subscriber.Domain;

namespace PerformanceTests.Scenarios;

public static class MultiTopicLongRunScenario
{
    // --- per-run state  ---
    private static readonly ConcurrentDictionary<string, TopicState> StateByTopic = new();

    private static readonly ConcurrentDictionary<string, List<IPublisher<TestMessage>>> PublishersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<ISubscriber<TestMessage>>> SubscribersByTopic = new();
    private static readonly ConcurrentDictionary<string, List<Task>> SubscriberProcessingTasksByTopic = new();

    // throughput CSV
    private static CancellationTokenSource? _csvCts;
    private static Task? _csvTask;

    private sealed class TopicState
    {
        public int DebugSamplesWritten;
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

        public long SeenCount
        {
            get { lock (_lock) return _seen; }
        }
    }

    private static void ResetAll()
    {
        StateByTopic.Clear();
        PublishersByTopic.Clear();
        SubscribersByTopic.Clear();
        SubscriberProcessingTasksByTopic.Clear();
    }

    private static string CleanTopicName(string topic)
        => topic.Replace("schema/topic/", "").Trim('/');

    // --- CSV: throughput subskrybera ---
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

        try { if (_csvTask != null) await _csvTask; }
        catch (OperationCanceledException) { }
        finally
        {
            _csvCts.Dispose();
            _csvCts = null;
            _csvTask = null;
        }
    }

    public static ScenarioProps[] Create(
        IPublisherFactory<TestMessage> publisherFactory,
        ISubscriberFactory<TestMessage> subscriberFactory,
        string brokerHost,
        int brokerPort,
        int brokerSubscriberPort,
        Uri schemaRegistryBaseAddress,
        TimeSpan schemaRegistryTimeout,
        uint maxPublisherQueueSize,
        int numTopics = 10,
        int publishersPerTopic = 1,
        int subscribersPerTopic = 1,
        int totalRate = 5000,
        int durationSeconds = 60 * 60 * 2,
        int drainSeconds = 10,
        int maxE2ESamplesPerTopic = 200_000,
        int? batchMaxBytesRaw = null,
        TimeSpan? batchMaxDelayRaw = null)
    {
        ResetAll();

        var batchMaxBytes = 4096;

        var batchMaxDelay = TimeSpan.FromMilliseconds(10);

        var ratePerTopic = Math.Max(1, totalRate / Math.Max(1, numTopics));
        var scenarios = new List<ScenarioProps>();

        var staleTolerance = TimeSpan.FromHours(24);

        for (int topicIndex = 1; topicIndex <= numTopics; topicIndex++)
        {
            var topicName = $"long-run-topic-{topicIndex:D2}";
            var cleanTopicName = CleanTopicName(topicName);
            long messageCounter = 0L;

            // =========================
            // PUBLISHER SCENARIO
            // =========================
            var publishScenario = Scenario.Create($"pub_{topicName}", async _ =>
            {
                if (!PublishersByTopic.TryGetValue(topicName, out var publishers) || publishers.Count == 0)
                    return Response.Fail<object>("No publishers");

                var publisherIndex = (int)(Interlocked.Read(ref messageCounter) % publishers.Count);
                var publisher = publishers[publisherIndex];

                var seq = Interlocked.Increment(ref messageCounter);
                var publishedAt = DateTime.UtcNow;

                var message = new TestMessage
                {
                    Id = (int)seq,
                    Timestamp = publishedAt.Ticks,
                    Content = $"Test {topicName} #{seq}",
                    Source = topicName,
                    SequenceNumber = seq
                };

                await publisher.PublishAsync(message);

                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));
                Interlocked.Increment(ref st.PublishedOk);

                return Response.Ok();
            })
            .WithInit(async _ =>
            {
                await EnsureSchemaForTestMessageAsync(schemaRegistryBaseAddress, schemaRegistryTimeout, cleanTopicName);

                var publishers = new List<IPublisher<TestMessage>>(publishersPerTopic);
                for (int i = 0; i < publishersPerTopic; i++)
                {
                    var pubOptions = new PublisherOptions(
                        MessageBrokerConnectionUri: new Uri($"messageBroker://{brokerHost}:{brokerPort}"),
                        SchemaRegistryConnectionUri: schemaRegistryBaseAddress,
                        SchemaRegistryTimeout: schemaRegistryTimeout,
                        Topic: cleanTopicName,
                        MaxPublisherQueueSize: maxPublisherQueueSize,
                        MaxSendAttempts: 5,
                        MaxRetryAttempts: 5,
                        BatchMaxBytes: batchMaxBytes,
                        BatchMaxDelay: batchMaxDelay
                    );

                    var publisher = publisherFactory.CreatePublisher(pubOptions);
                    await publisher.CreateConnection();
                    publishers.Add(publisher);
                }

                PublishersByTopic[topicName] = publishers;
            })
            .WithWarmUpDuration(TimeSpan.FromSeconds(60))
            .WithLoadSimulations(
                Simulation.Inject(rate: ratePerTopic, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(durationSeconds))
                //Simulation.Inject(rate: 500, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(15)),
                //Simulation.Inject(rate: 1000, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(60)),
                //Simulation.Inject(rate: 1500, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(15)),
                //Simulation.Inject(rate: 800, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromMinutes(60))
            )
            .WithClean(async _ =>
            {
                if (PublishersByTopic.TryRemove(topicName, out var publishers))
                {
                    foreach (var pub in publishers)
                        if (pub is IAsyncDisposable disp) { try { await disp.DisposeAsync(); } catch { } }
                }
            });

            scenarios.Add(publishScenario);

            // =========================
            // SUBSCRIBER SCENARIO 
            // =========================
            var subscribeScenario = Scenario.Create($"sub_{topicName}", async ctx =>
            {
                try
                {
                    await Task.Delay(1000, ctx.ScenarioCancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return Response.Ok(statusCode: "STOP", message: "Stopping", sizeBytes: 0);
                }

                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));

                var total = Interlocked.Read(ref st.ReceivedTotal);
                var e2e   = Interlocked.Read(ref st.ReceivedE2E);
                var stale = Interlocked.Read(ref st.SkippedStale);
                var skew  = Interlocked.Read(ref st.ClockSkewed);

                var msg = $"recv_total={total} e2e={e2e} stale={stale} skew={skew}";
                return Response.Ok(statusCode: "ALIVE", message: msg, sizeBytes: 0);
            })
            .WithInit(async ctx =>
            {
                await EnsureSchemaForTestMessageAsync(schemaRegistryBaseAddress, schemaRegistryTimeout, cleanTopicName);

                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));
                st.RunStartedUtc = DateTime.UtcNow;

                SubscriberProcessingTasksByTopic.TryAdd(topicName, new List<Task>());

                var subscribers = new List<ISubscriber<TestMessage>>(subscribersPerTopic);
                for (int i = 0; i < subscribersPerTopic; i++)
                {
                    var subOptions = new SubscriberOptions(
                        MessageBrokerConnectionUri: new Uri($"messageBroker://{brokerHost}:{brokerSubscriberPort}"),
                        SchemaRegistryConnectionUri: schemaRegistryBaseAddress,
                        Host: brokerHost,
                        Port: brokerSubscriberPort,
                        Topic: cleanTopicName,
                        MinMessageLength: 0,
                        MaxMessageLength: int.MaxValue,
                        MaxQueueSize: 65_536,
                        PollInterval: TimeSpan.FromMilliseconds(10),
                        SchemaRegistryTimeout: schemaRegistryTimeout,
                        MaxRetryAttempts: 3
                    );

                    var subscriber = subscriberFactory.CreateSubscriber(subOptions, message =>
                    {
                        var receivedAt = DateTime.UtcNow;
                        Interlocked.Increment(ref st.ReceivedTotal);
                        var publishedAt = new DateTime(message.Timestamp, DateTimeKind.Utc);
                        if (publishedAt < st.RunStartedUtc - staleTolerance)
                        {
                            Interlocked.Increment(ref st.SkippedStale);
                            return Task.CompletedTask;
                        }

                        var e2eMs = (receivedAt - publishedAt).TotalMilliseconds;
                        var idx = Interlocked.Increment(ref st.DebugSamplesWritten);
    if (idx <= 50)
    {
        try
        {
            var reportsDir = Path.Combine(Directory.GetCurrentDirectory(), "reports");
            Directory.CreateDirectory(reportsDir);

            var filePath = Path.Combine(reportsDir, $"e2e_debug_{topicName}.log");

            var line =
                $"#{idx}; raw_ts={message.Timestamp}; " +
                $"publishedAt={publishedAt:O}; " +
                $"receivedAt={receivedAt:O}; " +
                $"e2eMs={e2eMs:F2}{Environment.NewLine}";

            File.AppendAllText(filePath, line);
        }
        catch { /* ignore */ }
    }

                        if (e2eMs < 0)
                        {
                            Interlocked.Increment(ref st.ClockSkewed);
                            e2eMs = 0;
                        }

                        st.E2E.Add(e2eMs);
                        Interlocked.Increment(ref st.ReceivedE2E);

                        return Task.CompletedTask;
                    });

                    await subscriber.StartConnectionAsync();
                    var processingTask = subscriber.StartMessageProcessingAsync();

                    _ = processingTask.ContinueWith(
                        t => Console.WriteLine($"Subscriber processing faulted ({topicName}): {t.Exception}"),
                        TaskContinuationOptions.OnlyOnFaulted
                    );

                    SubscriberProcessingTasksByTopic[topicName].Add(processingTask);
                    subscribers.Add(subscriber);
                }

                SubscribersByTopic[topicName] = subscribers;
            })
            .WithWarmUpDuration(TimeSpan.FromSeconds(600))
            .WithLoadSimulations(
                Simulation.KeepConstant(copies: 1, during: TimeSpan.FromSeconds(durationSeconds + drainSeconds))
            )
            .WithClean(async ctx =>
            {
                var st = StateByTopic.GetOrAdd(topicName, _ => new TopicState(maxE2ESamplesPerTopic));

                var drainStart = DateTime.UtcNow;
                var last = Interlocked.Read(ref st.ReceivedE2E);

                while (DateTime.UtcNow - drainStart < TimeSpan.FromSeconds(60))
                {
                    await Task.Delay(1000);
                    var now = Interlocked.Read(ref st.ReceivedE2E);
                    if (now == last) break;
                    last = now;
                }

                if (SubscribersByTopic.TryRemove(topicName, out var subscribers))
                {
                    foreach (var sub in subscribers)
                        if (sub is IAsyncDisposable disp) { try { await disp.DisposeAsync(); } catch { } }
                }

                if (SubscriberProcessingTasksByTopic.TryRemove(topicName, out var tasks))
                    await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(2000));

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

                    var p50 = P(0.50);
                    var p95 = P(0.95);
                    var p99 = P(0.99);

                    Console.WriteLine($"  [{topicName}] PublishedOk={Interlocked.Read(ref st.PublishedOk):N0}");
                    Console.WriteLine($"  [{topicName}] ReceivedTotal={Interlocked.Read(ref st.ReceivedTotal):N0}");
                    Console.WriteLine($"  [{topicName}] ReceivedE2E={Interlocked.Read(ref st.ReceivedE2E):N0}");
                    Console.WriteLine($"  [{topicName}] SkippedStale={Interlocked.Read(ref st.SkippedStale):N0}");
                    Console.WriteLine($"  [{topicName}] ClockSkewed={Interlocked.Read(ref st.ClockSkewed):N0}");
                    Console.WriteLine($"  [{topicName}] E2E samples kept={samples.Count:N0}, seen={st.E2E.SeenCount:N0}");
                    Console.WriteLine($"  [{topicName}] E2E P50={p50:F2}ms P95={p95:F2}ms P99={p99:F2}ms");

                    SaveE2ELatencyStatsToFile(topicName, samples);
                }
                else
                {
                    Console.WriteLine($"  [{topicName}] No E2E samples");
                }
            });

            scenarios.Add(subscribeScenario);
        }

        return scenarios.ToArray();
    }

    private static async Task EnsureSchemaForTestMessageAsync(Uri baseAddress, TimeSpan timeout, string topic)
    {
        const string schema = """
        {
          "type": "record",
          "name": "TestMessage",
          "namespace": "PerformanceTests.Models",
          "fields": [
            { "name": "Id", "type": "int" },
            { "name": "Timestamp", "type": "long" },
            { "name": "Content", "type": "string" },
            { "name": "Source", "type": "string" },
            { "name": "SequenceNumber", "type": "long" }
          ]
        }
        """;

        using var http = new HttpClient { BaseAddress = baseAddress, Timeout = timeout };

        var endpoint = $"schema/topic/{topic}";
        var requestBody = new { Schema = schema };

        var json = JsonSerializer.Serialize(requestBody);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");

        var resp = await http.PostAsync(endpoint, content);
        if (resp.IsSuccessStatusCode) return;
        if (resp.StatusCode == HttpStatusCode.Conflict) return;

        var err = await resp.Content.ReadAsStringAsync();
        throw new Exception($"SchemaRegistry error: {resp.StatusCode} {err}");
    }

    private static void SaveE2ELatencyStatsToFile(string topicName, List<double> e2eLatencies)
    {
        try
        {
            var reportsDir = Path.Combine(Directory.GetCurrentDirectory(), "reports");
            Directory.CreateDirectory(reportsDir);

            var cleanTopic = topicName.Replace("schema/topic/", "").Trim('/');
            var fileName = $"e2e_{cleanTopic}_{DateTime.Now:yyyyMMdd_HHmmss}.txt";
            var filePath = Path.Combine(reportsDir, fileName);

            e2eLatencies.Sort();
            int n = e2eLatencies.Count;
            double P(double q)
            {
                var idx = (int)Math.Round(q * (n - 1));
                idx = Math.Clamp(idx, 0, n - 1);
                return e2eLatencies[idx];
            }

            var content =
                $"E2E Latency: {cleanTopic}\n" +
                $"Samples: {n:N0}\n" +
                $"P50: {P(0.50):F2}ms\n" +
                $"P75: {P(0.75):F2}ms\n" +
                $"P95: {P(0.95):F2}ms\n" +
                $"P99: {P(0.99):F2}ms\n";

            File.WriteAllText(filePath, content);
            Console.WriteLine($"  ✓ Saved: {fileName}");
        }
        catch { /* ignore */ }
    }

    public static void SaveGlobalE2EToFile()
    {
        Console.WriteLine($"[GLOBAL] topics={StateByTopic.Count}");
        var reportsDir = Path.Combine(Directory.GetCurrentDirectory(), "reports");
        Directory.CreateDirectory(reportsDir);

        var all = StateByTopic.Values.SelectMany(s => s.E2E.Snapshot()).ToList();
        if (all.Count == 0) return;
        all.Sort();

        double P(double q)
        {
            var idx = (int)Math.Round(q * (all.Count - 1));
            idx = Math.Clamp(idx, 0, all.Count - 1);
            return all[idx];
        }

        var ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var txtPath = Path.Combine(reportsDir, $"e2e_global_{ts}.txt");

        var txt =
            $"E2E Latency: GLOBAL\n" +
            $"Samples: {all.Count:N0}\n" +
            $"P50: {P(0.50):F2}ms\n" +
            $"P95: {P(0.95):F2}ms\n" +
            $"P75: {P(0.75):F2}ms\n" +
            $"P99: {P(0.99):F2}ms\n";

        File.WriteAllText(txtPath, txt);
    }
}
