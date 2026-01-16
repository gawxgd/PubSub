using System.Collections.Concurrent;

namespace BddE2eTests.Configuration.Performance;

/// <summary>
/// Collects performance metrics during load tests including timestamps for published/received messages,
/// calculates throughput, latency percentiles, message loss, and tracks memory usage.
/// </summary>
public class PerformanceMetricsCollector
{
    private readonly ConcurrentDictionary<long, DateTime> _publishedTimestamps = new();
    private readonly ConcurrentDictionary<long, DateTime> _receivedTimestamps = new();
    private readonly ConcurrentBag<MemorySample> _memorySamples = new();
    
    private DateTime _startTime;
    private DateTime _endTime;
    private bool _started;

    public void Start()
    {
        _startTime = DateTime.UtcNow;
        _started = true;
    }

    public void Stop()
    {
        _endTime = DateTime.UtcNow;
    }

    public void RecordPublished(long sequenceNumber, DateTime timestamp)
    {
        _publishedTimestamps.TryAdd(sequenceNumber, timestamp);
    }

    public void RecordReceived(long sequenceNumber, DateTime timestamp)
    {
        _receivedTimestamps.TryAdd(sequenceNumber, timestamp);
    }

    public void RecordMemorySample(long bytesUsed, DateTime timestamp)
    {
        _memorySamples.Add(new MemorySample(bytesUsed, timestamp));
    }

    public PerformanceReport GenerateReport(string scenarioName)
    {
        if (!_started)
        {
            _startTime = DateTime.UtcNow;
            _endTime = DateTime.UtcNow;
        }

        var duration = _endTime - _startTime;
        var totalPublished = _publishedTimestamps.Count;
        var totalReceived = _receivedTimestamps.Count;
        var messageLoss = totalPublished - totalReceived;
        var lossPercent = totalPublished > 0 ? (double)messageLoss / totalPublished * 100 : 0;

        var throughputPubPerSec = duration.TotalSeconds > 0 ? totalPublished / duration.TotalSeconds : 0;
        var throughputRecvPerSec = duration.TotalSeconds > 0 ? totalReceived / duration.TotalSeconds : 0;

        // Calculate latency samples
        var latencySamples = CalculateLatencySamples();
        var latencies = latencySamples
            .Select(s => s.Latency)
            .OrderBy(l => l)
            .ToList();

        // Calculate latency statistics
        var (latencyMin, latencyMax, latencyMean, latencyP50, latencyP75, latencyP95, latencyP99) = 
            CalculateLatencyStatistics(latencies);

        // Calculate memory statistics
        var (memoryMin, memoryMax, memoryAvg) = CalculateMemoryStatistics();

        // Calculate timeline data
        var timelineData = CalculateTimelineData();

        return new PerformanceReport(
            ScenarioName: scenarioName,
            StartTime: _startTime,
            EndTime: _endTime,
            Duration: duration,
            TotalPublished: totalPublished,
            TotalReceived: totalReceived,
            MessageLoss: messageLoss,
            LossPercent: lossPercent,
            ThroughputPubPerSec: throughputPubPerSec,
            ThroughputRecvPerSec: throughputRecvPerSec,
            LatencyMin: latencyMin,
            LatencyMax: latencyMax,
            LatencyMean: latencyMean,
            LatencyP50: latencyP50,
            LatencyP75: latencyP75,
            LatencyP95: latencyP95,
            LatencyP99: latencyP99,
            MemoryMinBytes: memoryMin,
            MemoryMaxBytes: memoryMax,
            MemoryAvgBytes: memoryAvg,
            LatencySamples: latencySamples,
            TimelineData: timelineData
        );
    }

    private List<LatencySample> CalculateLatencySamples()
    {
        var samples = new List<LatencySample>();

        foreach (var (seq, publishedTime) in _publishedTimestamps)
        {
            if (_receivedTimestamps.TryGetValue(seq, out var receivedTime))
            {
                var latency = receivedTime - publishedTime;
                samples.Add(new LatencySample(seq, publishedTime, receivedTime, latency));
            }
        }

        return samples.OrderBy(s => s.SequenceNumber).ToList();
    }

    private (TimeSpan Min, TimeSpan Max, TimeSpan Mean, TimeSpan P50, TimeSpan P75, TimeSpan P95, TimeSpan P99) 
        CalculateLatencyStatistics(List<TimeSpan> sortedLatencies)
    {
        if (sortedLatencies.Count == 0)
        {
            return (TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero);
        }

        var min = sortedLatencies[0];
        var max = sortedLatencies[^1];
        var mean = TimeSpan.FromTicks((long)sortedLatencies.Average(l => l.Ticks));
        var p50 = GetPercentile(sortedLatencies, 0.50);
        var p75 = GetPercentile(sortedLatencies, 0.75);
        var p95 = GetPercentile(sortedLatencies, 0.95);
        var p99 = GetPercentile(sortedLatencies, 0.99);

        return (min, max, mean, p50, p75, p95, p99);
    }

    private static TimeSpan GetPercentile(List<TimeSpan> sortedValues, double percentile)
    {
        if (sortedValues.Count == 0) return TimeSpan.Zero;
        
        var index = (int)Math.Ceiling(percentile * sortedValues.Count) - 1;
        index = Math.Max(0, Math.Min(index, sortedValues.Count - 1));
        return sortedValues[index];
    }

    private (long Min, long Max, long Avg) CalculateMemoryStatistics()
    {
        var samples = _memorySamples.ToList();
        if (samples.Count == 0)
        {
            return (0, 0, 0);
        }

        var min = samples.Min(s => s.BytesUsed);
        var max = samples.Max(s => s.BytesUsed);
        var avg = (long)samples.Average(s => s.BytesUsed);

        return (min, max, avg);
    }

    private List<TimelineDataPoint> CalculateTimelineData()
    {
        var timeline = new List<TimelineDataPoint>();
        
        if (!_started || _publishedTimestamps.IsEmpty)
        {
            return timeline;
        }

        var durationSeconds = (int)Math.Ceiling((_endTime - _startTime).TotalSeconds);
        
        for (int second = 0; second < durationSeconds; second++)
        {
            var windowStart = _startTime.AddSeconds(second);
            var windowEnd = _startTime.AddSeconds(second + 1);

            var publishedInWindow = _publishedTimestamps
                .Count(kvp => kvp.Value >= windowStart && kvp.Value < windowEnd);

            var receivedInWindow = _receivedTimestamps
                .Count(kvp => kvp.Value >= windowStart && kvp.Value < windowEnd);

            // Calculate average latency for messages received in this window
            var latenciesInWindow = new List<TimeSpan>();
            foreach (var (seq, receivedTime) in _receivedTimestamps.Where(kvp => kvp.Value >= windowStart && kvp.Value < windowEnd))
            {
                if (_publishedTimestamps.TryGetValue(seq, out var publishedTime))
                {
                    latenciesInWindow.Add(receivedTime - publishedTime);
                }
            }

            var avgLatencyMs = latenciesInWindow.Count > 0 
                ? latenciesInWindow.Average(l => l.TotalMilliseconds) 
                : 0;

            timeline.Add(new TimelineDataPoint(
                Second: second + 1,
                Published: publishedInWindow,
                Received: receivedInWindow,
                ThroughputPub: publishedInWindow,
                ThroughputRecv: receivedInWindow,
                AvgLatencyMs: avgLatencyMs
            ));
        }

        return timeline;
    }

    public void Reset()
    {
        _publishedTimestamps.Clear();
        _receivedTimestamps.Clear();
        _memorySamples.Clear();
        _started = false;
    }
}

public record LatencySample(
    long SequenceNumber,
    DateTime PublishedUtc,
    DateTime ReceivedUtc,
    TimeSpan Latency);

public record MemorySample(
    long BytesUsed,
    DateTime Timestamp);

public record TimelineDataPoint(
    int Second,
    int Published,
    int Received,
    double ThroughputPub,
    double ThroughputRecv,
    double AvgLatencyMs);

public record PerformanceReport(
    string ScenarioName,
    DateTime StartTime,
    DateTime EndTime,
    TimeSpan Duration,
    int TotalPublished,
    int TotalReceived,
    int MessageLoss,
    double LossPercent,
    double ThroughputPubPerSec,
    double ThroughputRecvPerSec,
    TimeSpan LatencyMin,
    TimeSpan LatencyMax,
    TimeSpan LatencyMean,
    TimeSpan LatencyP50,
    TimeSpan LatencyP75,
    TimeSpan LatencyP95,
    TimeSpan LatencyP99,
    long MemoryMinBytes,
    long MemoryMaxBytes,
    long MemoryAvgBytes,
    List<LatencySample> LatencySamples,
    List<TimelineDataPoint> TimelineData);
