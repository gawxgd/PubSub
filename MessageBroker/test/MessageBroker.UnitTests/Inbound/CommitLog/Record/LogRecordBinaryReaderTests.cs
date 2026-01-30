using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Inbound.CommitLog.Record;
using MessageBroker.Domain.Util;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog.Record;

public class LogRecordBinaryReaderTests
{
    [Fact]
    public void ReadFrom_Should_Read_Record_Written_By_Writer()
    {
        // Arrange
        var writer = new LogRecordBinaryWriter();
        var reader = new LogRecordBinaryReader();
        
        var originalOffset = 42UL;
        var originalTimestamp = 5000UL;
        var originalPayload = new byte[] { 1, 2, 3, 4, 5 };
        var baseTimestamp = 1000UL;
        
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);
        
        var record = new LogRecord(originalOffset, originalTimestamp, originalPayload);
        writer.WriteTo(record, bw, baseTimestamp);
        bw.Flush();

        // Act
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var readRecord = reader.ReadFrom(br, baseTimestamp);

        // Assert
        readRecord.Offset.Should().Be(originalOffset);
        readRecord.Timestamp.Should().Be(originalTimestamp);
        readRecord.Payload.ToArray().Should().BeEquivalentTo(originalPayload);
    }

    [Fact]
    public void ReadFrom_Should_Handle_Empty_Payload()
    {
        // Arrange
        var writer = new LogRecordBinaryWriter();
        var reader = new LogRecordBinaryReader();
        
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);
        
        var record = new LogRecord(10, 2000, Array.Empty<byte>());
        writer.WriteTo(record, bw, 1000);
        bw.Flush();

        // Act
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var readRecord = reader.ReadFrom(br, 1000);

        // Assert
        readRecord.Payload.Length.Should().Be(0);
    }

    [Fact]
    public void ReadFrom_Should_Handle_Large_Payload()
    {
        // Arrange
        var writer = new LogRecordBinaryWriter();
        var reader = new LogRecordBinaryReader();
        
        var payload = new byte[10000];
        Random.Shared.NextBytes(payload);
        
        var stream = new MemoryStream();
        var bw = new BinaryWriter(stream);
        
        var record = new LogRecord(100, 3000, payload);
        writer.WriteTo(record, bw, 2000);
        bw.Flush();

        // Act
        stream.Position = 0;
        var br = new BinaryReader(stream);
        var readRecord = reader.ReadFrom(br, 2000);

        // Assert
        readRecord.Payload.ToArray().Should().BeEquivalentTo(payload);
    }
}

