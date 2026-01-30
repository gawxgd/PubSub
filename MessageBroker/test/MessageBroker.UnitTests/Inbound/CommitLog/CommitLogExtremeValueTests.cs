using System.Text;
using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class CommitLogExtremeValueTests
{
    private readonly ILogRecordWriter _recordWriter;
    private readonly ILogRecordReader _recordReader;
    private readonly ICompressor _compressor;
    private readonly Encoding _encoding;

    public CommitLogExtremeValueTests()
    {
        _recordWriter = new LogRecordBinaryWriter();
        _recordReader = new LogRecordBinaryReader();
        _compressor = new NoopCompressor();
        _encoding = Encoding.UTF8;
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_MaxValue_Offset()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(ulong.MaxValue - 1, 1000, new byte[] { 1 }),
            new LogRecord(ulong.MaxValue, 1001, new byte[] { 2 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            ulong.MaxValue - 1,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.BaseOffset.Should().Be(ulong.MaxValue - 1);
        readBatch.Records.Should().HaveCount(2);
        readBatch.Records.First().Offset.Should().Be(ulong.MaxValue - 1);
        readBatch.Records.Last().Offset.Should().Be(ulong.MaxValue);
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_MaxValue_Timestamp()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, ulong.MaxValue, new byte[] { 1 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.First().Timestamp.Should().Be(ulong.MaxValue);
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Zero_Timestamp()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 0, new byte[] { 1 }),
            new LogRecord(2, 0, new byte[] { 2 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.Should().AllSatisfy(r => r.Timestamp.Should().Be(0));
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Single_Byte_Payload()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, new byte[] { 0xFF })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.First().Payload.ToArray().Should().BeEquivalentTo(new byte[] { 0xFF });
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Empty_Payload()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, Array.Empty<byte>())
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.First().Payload.ToArray().Should().BeEmpty();
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Large_Payload()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var largePayload = new byte[1024 * 1024];
        new Random(42).NextBytes(largePayload);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, largePayload)
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.First().Payload.ToArray().Should().BeEquivalentTo(largePayload);
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Many_Small_Records()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var recordCount = 10000;
        var records = new List<LogRecord>();
        for (int i = 0; i < recordCount; i++)
        {
            records.Add(new LogRecord((ulong)i, 1000 + (ulong)i, new byte[] { (byte)(i % 256) }));
        }
        
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.Should().HaveCount(recordCount);
        for (int i = 0; i < recordCount; i++)
        {
            readBatch.Records.ElementAt(i).Offset.Should().Be((ulong)i);
            readBatch.Records.ElementAt(i).Payload.ToArray()[0].Should().Be((byte)(i % 256));
        }
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Large_Timestamp_Deltas()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 0, new byte[] { 1 }),
            new LogRecord(2, ulong.MaxValue / 2, new byte[] { 2 }),
            new LogRecord(3, ulong.MaxValue - 1, new byte[] { 3 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.ElementAt(0).Timestamp.Should().Be(0);
        readBatch.Records.ElementAt(1).Timestamp.Should().Be(ulong.MaxValue / 2);
        readBatch.Records.ElementAt(2).Timestamp.Should().Be(ulong.MaxValue - 1);
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_All_Zero_Bytes_Payload()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var payload = new byte[1000];
        Array.Fill(payload, (byte)0);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, payload)
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.First().Payload.ToArray().Should().AllSatisfy(b => b.Should().Be(0));
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_All_Max_Bytes_Payload()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var payload = new byte[1000];
        Array.Fill(payload, (byte)0xFF);
        
        var records = new List<LogRecord>
        {
            new LogRecord(1, 1000, payload)
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.First().Payload.ToArray().Should().AllSatisfy(b => b.Should().Be(0xFF));
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Sequential_Offsets_Starting_At_Zero()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>();
        for (ulong i = 0; i < 100; i++)
        {
            records.Add(new LogRecord(i, 1000 + i, new byte[] { (byte)i }));
        }
        
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.Should().HaveCount(100);
        for (ulong i = 0; i < 100; i++)
        {
            readBatch.Records.ElementAt((int)i).Offset.Should().Be(i);
        }
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Very_Large_Batch_With_Compression()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var recordCount = 5000;
        var records = new List<LogRecord>();
        
        var repetitivePayload = new byte[100];
        Array.Fill(repetitivePayload, (byte)'A');
        
        for (int i = 0; i < recordCount; i++)
        {
            records.Add(new LogRecord((ulong)i, 1000 + (ulong)i, repetitivePayload));
        }
        
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            true
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.Should().HaveCount(recordCount);
        readBatch.Compressed.Should().BeTrue();
    }

    [Fact]
    public void LogRecordBatch_Should_Handle_Sparse_Offsets()
    {
        // Arrange
        var writer = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
        var reader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
        
        var records = new List<LogRecord>
        {
            new LogRecord(0, 1000, new byte[] { 1 }),
            new LogRecord(1000, 1001, new byte[] { 2 }),
            new LogRecord(1000000, 1002, new byte[] { 3 }),
            new LogRecord(ulong.MaxValue - 1, 1003, new byte[] { 4 })
        };
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );
        
        var stream = new MemoryStream();

        // Act
        writer.WriteTo(batch, stream);
        stream.Position = 0;
        var readBatch = reader.ReadBatch(stream);

        // Assert
        readBatch.Records.Should().HaveCount(4);
        readBatch.Records.ElementAt(0).Offset.Should().Be(0);
        readBatch.Records.ElementAt(1).Offset.Should().Be(1000);
        readBatch.Records.ElementAt(2).Offset.Should().Be(1000000);
        readBatch.Records.ElementAt(3).Offset.Should().Be(ulong.MaxValue - 1);
    }
}

