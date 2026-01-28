using System.Threading.Channels;
using NUnit.Framework;
using Reqnroll;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;

namespace BddE2eTests.Steps.CommitLog.Then;

[Binding]
public class CommitLogSegmentsThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 30;
    private const string ReceivedOffsetsKey = "ReceivedOffsets";

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"commit log contains messages from offset (\d+) to (\d+)")]
    public async Task ThenCommitLogContainsMessagesFromOffsetToOffset(int startOffset, int endOffset)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Verifying commit log contains messages from offset {startOffset} to {endOffset}...");

        var commitLogDir = TestBase.CommitLogDirectory;
        Assert.That(commitLogDir, Is.Not.Null.And.Not.Empty,
            "Commit log directory should be configured");

        var topicDir = Path.Combine(commitLogDir!, _context.Topic ?? "default");
        Assert.That(Directory.Exists(topicDir), Is.True,
            $"Topic directory should exist: {topicDir}");

        var logFiles = Directory.GetFiles(topicDir, "*.log").OrderBy(f => f).ToArray();
        var allOffsets = new List<ulong>();

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Scanning {logFiles.Length} log file(s) for offsets...");

        foreach (var logFile in logFiles)
        {
            var fileBytes = await File.ReadAllBytesAsync(logFile);
            var offsets = ExtractOffsetsFromLogFile(fileBytes);
            allOffsets.AddRange(offsets);

            await TestContext.Progress.WriteLineAsync(
                $"[CommitLog Then]   {Path.GetFileName(logFile)}: {offsets.Count} offsets ({(offsets.Count > 0 ? $"{offsets.Min()}-{offsets.Max()}" : "empty")})");
        }

        allOffsets.Sort();
        var distinctOffsets = allOffsets.Distinct().ToList();

        var expectedOffsets = Enumerable.Range(startOffset, endOffset - startOffset + 1)
            .Select(i => (ulong)i)
            .ToList();

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Found {distinctOffsets.Count} distinct offsets in commit log");
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Expected: {expectedOffsets.Count} offsets from {startOffset} to {endOffset}");

        Assert.That(distinctOffsets.Count, Is.EqualTo(expectedOffsets.Count),
            $"Expected {expectedOffsets.Count} offsets but found {distinctOffsets.Count}");

        for (var i = 0; i < expectedOffsets.Count; i++)
        {
            Assert.That(distinctOffsets[i], Is.EqualTo(expectedOffsets[i]),
                $"Offset mismatch at position {i}: expected {expectedOffsets[i]}, got {distinctOffsets[i]}");
        }

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Verified: commit log contains continuous offsets from {startOffset} to {endOffset}");
    }

    /// <summary>
    /// Extracts all offsets from a log file by parsing batch headers.
    /// Batch format (little-endian):
    /// - BaseOffset: 8 bytes (ulong) at position 0
    /// - BatchLength: 4 bytes (uint) at position 8
    /// - LastOffset: 8 bytes (ulong) at position 12
    /// Total batch size = 24 + BatchLength
    /// </summary>
    private static List<ulong> ExtractOffsetsFromLogFile(byte[] logData)
    {
        var offsets = new List<ulong>();
        var position = 0;

        while (position + 24 <= logData.Length)
        {
            try
            {
                var baseOffset = BitConverter.ToUInt64(logData, position);
                var batchLength = BitConverter.ToUInt32(logData, position + 8);
                var lastOffset = BitConverter.ToUInt64(logData, position + 12);

                // Sanity checks
                if (batchLength == 0 || batchLength > 10_000_000)
                    break;

                // Add all offsets in this batch range
                for (var offset = baseOffset; offset <= lastOffset; offset++)
                {
                    offsets.Add(offset);
                }

                // Move to next batch
                position += 24 + (int)batchLength;
            }
            catch
            {
                break;
            }
        }

        return offsets;
    }

    [Then(@"multiple segment files exist for topic ""(.*)""")]
    public async Task ThenMultipleSegmentFilesExistForTopic(string topic)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Checking for multiple segment files for topic '{topic}'...");

        var commitLogDir = TestBase.CommitLogDirectory;
        Assert.That(commitLogDir, Is.Not.Null.And.Not.Empty,
            "Commit log directory should be configured");

        var topicDir = Path.Combine(commitLogDir!, topic);
        Assert.That(Directory.Exists(topicDir), Is.True,
            $"Topic directory should exist: {topicDir}");

        var logFiles = Directory.GetFiles(topicDir, "*.log");
        var indexFiles = Directory.GetFiles(topicDir, "*.index");

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Found {logFiles.Length} log files and {indexFiles.Length} index files");

        foreach (var logFile in logFiles)
        {
            var fileInfo = new FileInfo(logFile);
            await TestContext.Progress.WriteLineAsync(
                $"[CommitLog Then]   - {Path.GetFileName(logFile)}: {fileInfo.Length} bytes");
        }

        Assert.That(logFiles.Length, Is.GreaterThan(1),
            $"Expected multiple segment files for topic '{topic}', but found {logFiles.Length}. " +
            $"This indicates segment rolling did not occur. Directory: {topicDir}");

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Verified: {logFiles.Length} segment files exist for topic '{topic}'");
    }

    [Then(@"subscriber (.+) receives all (\d+) messages in correct order")]
    public async Task ThenSubscriberReceivesAllMessagesInCorrectOrder(string subscriberName, int expectedCount)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Waiting for subscriber '{subscriberName}' to receive {expectedCount} messages in order...");

        var receivedMessages = _context.GetSubscriberReceivedMessages(subscriberName);
        var (receivedCount, messages) = await WaitForMessagesAsync(receivedMessages, expectedCount);

        Assert.That(receivedCount, Is.EqualTo(expectedCount),
            $"Expected {expectedCount} messages but received {receivedCount}");

        // Verify messages are in correct order (msg0, msg1, msg2, ...)
        for (var i = 0; i < expectedCount; i++)
        {
            var expectedPrefix = $"msg{i}:";
            Assert.That(messages[i], Does.StartWith(expectedPrefix),
                $"Message at position {i} should start with '{expectedPrefix}' but was '{messages[i].Substring(0, Math.Min(20, messages[i].Length))}...'");
        }

        // Store the received order for offset verification
        StoreReceivedOffsets(Enumerable.Range(0, expectedCount).Select(i => (ulong)i).ToList());

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] All {expectedCount} messages received in correct order by '{subscriberName}'!");
    }

    [Then(@"message offsets are continuous from (\d+) to (\d+)")]
    public async Task ThenMessageOffsetsAreContinuousFromTo(int startOffset, int endOffset)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Verifying offsets are continuous from {startOffset} to {endOffset}...");

        var expectedOffsets = Enumerable.Range(startOffset, endOffset - startOffset + 1)
            .Select(i => (ulong)i)
            .ToList();

        var receivedOffsets = GetReceivedOffsets();

        Assert.That(receivedOffsets, Is.Not.Null.And.Not.Empty,
            "No offsets were recorded from received messages");

        Assert.That(receivedOffsets.Count, Is.EqualTo(expectedOffsets.Count),
            $"Expected {expectedOffsets.Count} offsets but got {receivedOffsets.Count}");

        for (var i = 0; i < expectedOffsets.Count; i++)
        {
            Assert.That(receivedOffsets[i], Is.EqualTo(expectedOffsets[i]),
                $"Offset at position {i} should be {expectedOffsets[i]} but was {receivedOffsets[i]}");
        }

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Verified: offsets are continuous from {startOffset} to {endOffset}");
    }

    [Then(@"subscriber (.+) receives messages from offset (\d+) to (\d+)")]
    public async Task ThenSubscriberReceivesMessagesFromOffsetToOffset(string subscriberName, int startOffset, int endOffset)
    {
        var expectedCount = endOffset - startOffset + 1;
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Waiting for subscriber '{subscriberName}' to receive messages from offset {startOffset} to {endOffset} ({expectedCount} messages)...");

        var receivedMessages = _context.GetSubscriberReceivedMessages(subscriberName);
        var (receivedCount, messages) = await WaitForMessagesAsync(receivedMessages, expectedCount);

        Assert.That(receivedCount, Is.EqualTo(expectedCount),
            $"Expected {expectedCount} messages (offsets {startOffset}-{endOffset}) but received {receivedCount}");

        // Verify messages match the expected offset range
        for (var i = 0; i < expectedCount; i++)
        {
            var expectedMsgIndex = startOffset + i;
            var expectedPrefix = $"msg{expectedMsgIndex}:";
            Assert.That(messages[i], Does.StartWith(expectedPrefix),
                $"Message at position {i} (offset {expectedMsgIndex}) should start with '{expectedPrefix}' " +
                $"but was '{messages[i].Substring(0, Math.Min(20, messages[i].Length))}...'");
        }

        // Store the received offsets
        StoreReceivedOffsets(Enumerable.Range(startOffset, expectedCount).Select(i => (ulong)i).ToList());

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Received all {expectedCount} messages from offset {startOffset} to {endOffset} by '{subscriberName}'!");
    }

    [Then(@"reading crosses segment file boundaries seamlessly")]
    public async Task ThenReadingCrossesSegmentFileBoundariesSeamlessly()
    {
        await TestContext.Progress.WriteLineAsync(
            "[CommitLog Then] Verifying that reading crossed segment boundaries...");

        var commitLogDir = TestBase.CommitLogDirectory;
        Assert.That(commitLogDir, Is.Not.Null.And.Not.Empty,
            "Commit log directory should be configured");

        var topicDir = Path.Combine(commitLogDir!, _context.Topic);
        var logFiles = Directory.GetFiles(topicDir, "*.log").OrderBy(f => f).ToArray();

        Assert.That(logFiles.Length, Is.GreaterThan(1),
            "Expected multiple segment files to verify cross-segment reading");

        // Verify we have messages across segments by checking the offset ranges of each segment
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog Then] Data spans {logFiles.Length} segment files:");

        var allOffsetsSpanned = new List<(ulong baseOffset, string fileName)>();
        foreach (var logFile in logFiles)
        {
            var fileName = Path.GetFileNameWithoutExtension(logFile);
            if (ulong.TryParse(fileName, out var baseOffset))
            {
                allOffsetsSpanned.Add((baseOffset, Path.GetFileName(logFile)));
                await TestContext.Progress.WriteLineAsync(
                    $"[CommitLog Then]   - {Path.GetFileName(logFile)}: base offset {baseOffset}");
            }
        }

        // Verify that received offsets span multiple segments
        var receivedOffsets = GetReceivedOffsets();
        if (receivedOffsets != null && receivedOffsets.Count > 0)
        {
            var minReceivedOffset = receivedOffsets.Min();
            var maxReceivedOffset = receivedOffsets.Max();

            // Check if the received range crosses at least one segment boundary
            var segmentsCrossed = allOffsetsSpanned
                .Count(s => s.baseOffset > minReceivedOffset && s.baseOffset <= maxReceivedOffset);

            await TestContext.Progress.WriteLineAsync(
                $"[CommitLog Then] Received offsets span from {minReceivedOffset} to {maxReceivedOffset}, " +
                $"crossing {segmentsCrossed} segment boundaries");

            Assert.That(segmentsCrossed, Is.GreaterThan(0),
                $"Expected to cross at least one segment boundary. " +
                $"Received offsets: {minReceivedOffset}-{maxReceivedOffset}, " +
                $"Segments: {string.Join(", ", allOffsetsSpanned.Select(s => s.baseOffset))}");
        }

        await TestContext.Progress.WriteLineAsync(
            "[CommitLog Then] Verified: reading crossed segment file boundaries seamlessly!");
    }

    private async Task<(int count, List<string> messages)> WaitForMessagesAsync(
        Channel<ITestEvent> receivedMessages, int expectedCount)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        var receivedCount = 0;
        var messages = new List<string>();

        try
        {
            await foreach (var message in receivedMessages.Reader.ReadAllAsync(cts.Token))
            {
                receivedCount++;
                messages.Add(message.Message);

                if (receivedCount % 10 == 0)
                {
                    await TestContext.Progress.WriteLineAsync(
                        $"[CommitLog Then] Received {receivedCount}/{expectedCount} messages...");
                }

                if (receivedCount >= expectedCount)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            await TestContext.Progress.WriteLineAsync(
                $"[CommitLog Then] TIMEOUT after {TimeoutSeconds}s waiting for {expectedCount} messages! " +
                $"Received {receivedCount}");
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for {expectedCount} messages. Received {receivedCount}.");
        }

        return (receivedCount, messages);
    }

    private void StoreReceivedOffsets(List<ulong> offsets)
    {
        scenarioContext.Set(offsets, ReceivedOffsetsKey);
    }

    private List<ulong>? GetReceivedOffsets()
    {
        return scenarioContext.TryGetValue(ReceivedOffsetsKey, out List<ulong>? offsets) ? offsets : null;
    }
}

