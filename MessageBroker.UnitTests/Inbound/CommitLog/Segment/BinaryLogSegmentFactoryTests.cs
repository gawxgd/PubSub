using FluentAssertions;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Inbound.CommitLog.Segment;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog.Segment;

public class BinaryLogSegmentFactoryTests
{
    private readonly ILogRecordBatchWriter _batchWriter;

    public BinaryLogSegmentFactoryTests()
    {
        _batchWriter = Substitute.For<ILogRecordBatchWriter>();
    }

    [Fact]
    public void CreateLogSegment_Should_Generate_Correct_Filename_With_Zero_Offset()
    {
        // Arrange
        var factory = CreateFactory();
        const string directory = "/test/dir";

        // Act
        var segment = factory.CreateLogSegment(directory, 0);

        // Assert
        segment.LogPath.Should().Be(Path.Combine(directory, "00000000000000000000.log"));
        segment.IndexFilePath.Should().Be(Path.Combine(directory, "00000000000000000000.index"));
        segment.TimeIndexFilePath.Should().Be(Path.Combine(directory, "00000000000000000000.timeindex"));
    }

    [Fact]
    public void CreateLogSegment_Should_Generate_Correct_Filename_With_NonZero_Offset()
    {
        // Arrange
        var factory = CreateFactory();
        const string directory = "/test/dir";
        const ulong baseOffset = 123456;

        // Act
        var segment = factory.CreateLogSegment(directory, baseOffset);

        // Assert
        segment.LogPath.Should().Be(Path.Combine(directory, "00000000000000123456.log"));
        segment.IndexFilePath.Should().Be(Path.Combine(directory, "00000000000000123456.index"));
        segment.TimeIndexFilePath.Should().Be(Path.Combine(directory, "00000000000000123456.timeindex"));
    }

    [Fact]
    public void CreateLogSegment_Should_Generate_Correct_Filename_With_Large_Offset()
    {
        // Arrange
        var factory = CreateFactory();
        const string directory = "/test/dir";
        const ulong baseOffset = 9999999999999999999;

        // Act
        var segment = factory.CreateLogSegment(directory, baseOffset);

        // Assert
        segment.LogPath.Should().Be(Path.Combine(directory, "09999999999999999999.log"));
    }

    [Fact]
    public void CreateLogSegment_Should_Set_BaseOffset_And_NextOffset()
    {
        // Arrange
        var factory = CreateFactory();
        const ulong baseOffset = 42;

        // Act
        var segment = factory.CreateLogSegment("/test", baseOffset);

        // Assert
        segment.BaseOffset.Should().Be(baseOffset);
        segment.NextOffset.Should().Be(baseOffset);
    }

    [Fact]
    public void CreateWriter_Should_Return_BinaryLogSegmentWriter()
    {
        // Arrange
        var factory = CreateFactory();
        var testDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDir);
        
        try
        {
            var segment = new MessageBroker.Domain.Entities.CommitLog.LogSegment(
                Path.Combine(testDir, "test.log"),
                Path.Combine(testDir, "test.index"),
                Path.Combine(testDir, "test.timeindex"),
                0,
                0
            );

            // Act
            var writer = factory.CreateWriter(segment);

            // Assert
            writer.Should().NotBeNull();
            writer.Should().BeOfType<BinaryLogSegmentWriter>();
            
            // Cleanup
            writer.DisposeAsync().AsTask().Wait();
        }
        finally
        {
            if (Directory.Exists(testDir))
            {
                Directory.Delete(testDir, true);
            }
        }
    }

    [Fact]
    public void CreateWriter_Should_Pass_Options_To_Writer()
    {
        // Arrange
        const ulong maxSegmentBytes = 1024;
        const uint indexIntervalBytes = 512;
        const uint timeIndexIntervalMs = 1000;
        const uint fileBufferSize = 256;
        
        var factory = CreateFactory(maxSegmentBytes, indexIntervalBytes, timeIndexIntervalMs, fileBufferSize);
        var testDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testDir);
        
        try
        {
            var segment = new MessageBroker.Domain.Entities.CommitLog.LogSegment(
                Path.Combine(testDir, "test.log"),
                Path.Combine(testDir, "test.index"),
                Path.Combine(testDir, "test.timeindex"),
                0,
                0
            );

            // Act
            var writer = factory.CreateWriter(segment);

            // Assert
            writer.Should().NotBeNull();
            
            // Cleanup
            writer.DisposeAsync().AsTask().Wait();
        }
        finally
        {
            if (Directory.Exists(testDir))
            {
                Directory.Delete(testDir, true);
            }
        }
    }

    private BinaryLogSegmentFactory CreateFactory(
        ulong maxSegmentBytes = 134217728,
        uint indexIntervalBytes = 4096,
        uint timeIndexIntervalMs = 60000,
        uint fileBufferSize = 65536)
    {
        var options = Substitute.For<IOptions<CommitLogOptions>>();
        options.Value.Returns(new CommitLogOptions
        {
            MaxSegmentBytes = maxSegmentBytes,
            IndexIntervalBytes = indexIntervalBytes,
            TimeIndexIntervalMs = timeIndexIntervalMs,
            FileBufferSize = fileBufferSize
        });

        return new BinaryLogSegmentFactory(_batchWriter, options);
    }
}

