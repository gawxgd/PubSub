using BddE2eTests.Configuration.TestEvents;
using Publisher.Domain.Port;
using Subscriber.Domain;

namespace BddE2eTests.Configuration.Performance;

/// <summary>
/// Executes constant load at a specified rate for a specified duration.
/// Uses PeriodicTimer for precise rate limiting.
/// </summary>
public class ConstantLoadRunner
{
    private readonly PerformanceMetricsCollector _collector;
    private long _sequenceNumber;
    private Timer? _memoryTimer;

    public ConstantLoadRunner(PerformanceMetricsCollector collector)
    {
        _collector = collector;
    }

    /// <summary>
    /// Runs a constant load test at the specified rate.
    /// </summary>
    /// <param name="publisher">The publisher to send messages through</param>
    /// <param name="onMessageReceived">Callback to hook into subscriber message reception</param>
    /// <param name="rate">Messages per second to publish</param>
    /// <param name="durationSeconds">How long to run the test</param>
    /// <param name="topic">Topic to publish to</param>
    /// <param name="ct">Cancellation token</param>
    public async Task RunAsync(
        IPublisher<TestEvent> publisher,
        int rate,
        int durationSeconds,
        string topic,
        CancellationToken ct = default)
    {
        _sequenceNumber = 0;
        var totalMessages = rate * durationSeconds;
        var intervalMs = 1000.0 / rate;

        Console.WriteLine($"[LoadRunner] Starting constant load test:");
        Console.WriteLine($"[LoadRunner]   Rate: {rate} msg/s");
        Console.WriteLine($"[LoadRunner]   Duration: {durationSeconds} seconds");
        Console.WriteLine($"[LoadRunner]   Total expected messages: {totalMessages}");
        Console.WriteLine($"[LoadRunner]   Interval between messages: {intervalMs:F2} ms");

        // Start memory sampling
        StartMemorySampling();

        _collector.Start();

        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(intervalMs));
            var messagesPublished = 0;
            var startTime = DateTime.UtcNow;
            var endTime = startTime.AddSeconds(durationSeconds);

            while (!ct.IsCancellationRequested && DateTime.UtcNow < endTime)
            {
                var seq = Interlocked.Increment(ref _sequenceNumber);
                var publishTime = DateTime.UtcNow;

                var evt = new TestEvent
                {
                    Message = $"perf-{seq}",
                    Topic = topic
                };

                try
                {
                    await publisher.PublishAsync(evt);
                    _collector.RecordPublished(seq, publishTime);
                    messagesPublished++;

                    if (messagesPublished % (rate * 10) == 0) // Log every 10 seconds worth
                    {
                        var elapsed = DateTime.UtcNow - startTime;
                        var actualRate = messagesPublished / elapsed.TotalSeconds;
                        Console.WriteLine($"[LoadRunner] Progress: {messagesPublished}/{totalMessages} messages ({elapsed.TotalSeconds:F1}s, {actualRate:F1} msg/s actual)");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[LoadRunner] Error publishing message {seq}: {ex.Message}");
                }

                // Wait for next interval
                try
                {
                    await timer.WaitForNextTickAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            var totalElapsed = DateTime.UtcNow - startTime;
            Console.WriteLine($"[LoadRunner] Publishing completed: {messagesPublished} messages in {totalElapsed.TotalSeconds:F1}s");
        }
        finally
        {
            StopMemorySampling();
            _collector.Stop();
        }
    }

    /// <summary>
    /// Creates a message handler that records received messages to the collector.
    /// Use this to wrap subscriber message handling.
    /// </summary>
    public Func<TestEvent, Task> CreateMessageHandler()
    {
        return async (TestEvent evt) =>
        {
            var receivedTime = DateTime.UtcNow;
            
            // Extract sequence number from message (format: "perf-{seq}")
            if (evt.Message.StartsWith("perf-") && 
                long.TryParse(evt.Message.Substring(5), out var seq))
            {
                _collector.RecordReceived(seq, receivedTime);
            }
            
            await Task.CompletedTask;
        };
    }

    private void StartMemorySampling()
    {
        _memoryTimer = new Timer(_ =>
        {
            var memoryUsed = GC.GetTotalMemory(false);
            _collector.RecordMemorySample(memoryUsed, DateTime.UtcNow);
        }, null, TimeSpan.Zero, TimeSpan.FromSeconds(1));
    }

    private void StopMemorySampling()
    {
        _memoryTimer?.Dispose();
        _memoryTimer = null;
    }
}
