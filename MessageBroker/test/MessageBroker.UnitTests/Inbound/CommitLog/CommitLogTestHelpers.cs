using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

/// <summary>
/// Shared test helper methods for comparing commit log entities
/// </summary>
public static class CommitLogTestHelpers
{
    /// <summary>
    /// Asserts that two LogRecords are equal by comparing all fields
    /// </summary>
    public static void AssertLogRecordsEqual(LogRecord expected, LogRecord actual, string because = "")
    {
        actual.Offset.Should().Be(expected.Offset, $"{because} - offsets should match");
        actual.Timestamp.Should().Be(expected.Timestamp, $"{because} - timestamps should match");
        actual.Payload.ToArray().Should().BeEquivalentTo(expected.Payload.ToArray(), $"{because} - payloads should match");
    }

    /// <summary>
    /// Asserts that two LogRecordBatches are equal by comparing all fields and all records
    /// </summary>
    public static void AssertBatchesEqual(LogRecordBatch expected, LogRecordBatch? actual, string because = "")
    {
        actual.Should().NotBeNull($"{because} - actual batch should not be null");
        actual!.MagicNumber.Should().Be(expected.MagicNumber, $"{because} - magic numbers should match");
        actual.BaseOffset.Should().Be(expected.BaseOffset, $"{because} - base offsets should match");
        actual.Compressed.Should().Be(expected.Compressed, $"{because} - compression flags should match");
        actual.Records.Should().HaveCount(expected.Records.Count, $"{because} - record counts should match");
        
        var expectedRecords = expected.Records.ToList();
        var actualRecords = actual.Records.ToList();
        
        for (int i = 0; i < expectedRecords.Count; i++)
        {
            AssertLogRecordsEqual(expectedRecords[i], actualRecords[i], $"{because} - record {i}");
        }
    }

    /// <summary>
    /// Asserts that a batch contains the expected records with correct offsets and payloads
    /// </summary>
    public static void AssertBatchContainsRecords(
        LogRecordBatch? batch,
        ulong expectedBaseOffset,
        params byte[][] expectedPayloads)
    {
        batch.Should().NotBeNull("batch should not be null");
        batch!.BaseOffset.Should().Be(expectedBaseOffset, "base offset should match");
        batch.Records.Should().HaveCount(expectedPayloads.Length, "record count should match");
        
        var records = batch.Records.ToList();
        for (int i = 0; i < expectedPayloads.Length; i++)
        {
            records[i].Offset.Should().Be(expectedBaseOffset + (ulong)i, $"record {i} offset should be sequential");
            records[i].Payload.ToArray().Should().BeEquivalentTo(expectedPayloads[i], $"record {i} payload should match");
        }
    }

    /// <summary>
    /// Asserts that all records in a batch have sequential offsets starting from base offset
    /// </summary>
    public static void AssertSequentialOffsets(LogRecordBatch? batch, string because = "")
    {
        batch.Should().NotBeNull($"{because} - batch should not be null");
        
        var records = batch!.Records.ToList();
        for (int i = 0; i < records.Count; i++)
        {
            records[i].Offset.Should().Be(batch.BaseOffset + (ulong)i, 
                $"{because} - record {i} should have sequential offset");
        }
    }

    /// <summary>
    /// Asserts that all records in a batch have timestamps within a reasonable range
    /// </summary>
    public static void AssertTimestampsValid(LogRecordBatch? batch, string because = "")
    {
        batch.Should().NotBeNull($"{because} - batch should not be null");
        
        var records = batch!.Records.ToList();
        foreach (var record in records)
        {
            record.Timestamp.Should().BeGreaterThan(0, $"{because} - timestamps should be greater than 0");
        }
    }
}

