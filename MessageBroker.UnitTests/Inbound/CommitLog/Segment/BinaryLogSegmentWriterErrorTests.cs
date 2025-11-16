// using System.Text;
// using FluentAssertions;
// using MessageBroker.Domain.Entities.CommitLog;
// using MessageBroker.Domain.Port.CommitLog.Record;
// using MessageBroker.Domain.Port.CommitLog.RecordBatch;
// using MessageBroker.Inbound.CommitLog.Segment;
// using NSubstitute;
// using NSubstitute.ExceptionExtensions;
// using Xunit;
//
// namespace MessageBroker.UnitTests.Inbound.CommitLog.Segment;
//
// public class BinaryLogSegmentWriterErrorTests : IDisposable
// {
//     private readonly string _testDirectory;
//     private readonly ILogRecordBatchWriter _batchWriter;
//
//     public BinaryLogSegmentWriterErrorTests()
//     {
//         _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
//         Directory.CreateDirectory(_testDirectory);
//         _batchWriter = Substitute.For<ILogRecordBatchWriter>();
//     }
//
//     [Fact]
//     public void Constructor_Should_Throw_When_Directory_Creation_Fails()
//     {
//         // Arrange - Use a path that cannot be created (e.g., nested under a file)
//         var invalidPath = Path.Combine(_testDirectory, "file.txt");
//         File.WriteAllText(invalidPath, "test");
//         
//         var logPath = Path.Combine(invalidPath, "nested", "00000000000000000000.log");
//         var segment = new LogSegment(
//             logPath,
//             Path.Combine(invalidPath, "nested", "00000000000000000000.index"),
//             Path.Combine(invalidPath, "nested", "00000000000000000000.timeindex"),
//             0,
//             0
//         );
//
//         // Act
//         var act = () => new BinaryLogSegmentWriter(
//             _batchWriter,
//             segment,
//             1024 * 1024,
//             1024,
//             1000,
//             4096
//         );
//
//         // Assert
//         act.Should().Throw<IOException>()
//             .Or.Throw<UnauthorizedAccessException>()
//             .Or.Throw<NotSupportedException>();
//     }
//
//     [Fact]
//     public async Task AppendAsync_Should_Throw_When_Writer_Fails()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         
//         _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
//             .Do(_ => throw new IOException("Disk full"));
//
//         var writer = CreateWriter(segment);
//         var batch = CreateTestBatch();
//
//         // Act
//         var act = async () => await writer.AppendAsync(batch);
//
//         // Assert
//         await act.Should().ThrowAsync<IOException>()
//             .WithMessage("*Disk full*");
//     }
//
//     [Fact]
//     public async Task AppendAsync_Should_Propagate_UnauthorizedAccessException()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         
//         _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
//             .Do(_ => throw new UnauthorizedAccessException("Permission denied"));
//
//         var writer = CreateWriter(segment);
//         var batch = CreateTestBatch();
//
//         // Act
//         var act = async () => await writer.AppendAsync(batch);
//
//         // Assert
//         await act.Should().ThrowAsync<UnauthorizedAccessException>()
//             .WithMessage("*Permission denied*");
//     }
//
//     [Fact]
//     public async Task AppendAsync_Should_Handle_Stream_Disposal_During_Write()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         var writer = CreateWriter(segment);
//         var batch = CreateTestBatch();
//
//         // First append succeeds
//         _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
//             .Do(call => call.Arg<Stream>().Write(new byte[10]));
//
//         await writer.AppendAsync(batch);
//
//         // Dispose the writer
//         await writer.DisposeAsync();
//
//         // Act - Try to append after disposal
//         var act = async () => await writer.AppendAsync(batch);
//
//         // Assert
//         await act.Should().ThrowAsync<ObjectDisposedException>();
//     }
//
//     [Fact]
//     public async Task Roll_Should_Handle_Disposal_Errors_Gracefully()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         
//         _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
//             .Do(call => call.Arg<Stream>().Write(new byte[1024]));
//
//         var writer = CreateWriter(segment, maxSegmentBytes: 500);
//         var batch = CreateTestBatch(recordCount: 10);
//
//         // Act - Force roll by exceeding maxSegmentBytes
//         await writer.AppendAsync(batch);
//         
//         // Assert - Should have rolled without throwing
//         writer.ShouldRoll().Should().BeFalse("segment should have rolled and reset state");
//     }
//
//     [Fact]
//     public async Task AppendAsync_Should_Handle_Partial_Write_Failure()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         var callCount = 0;
//         
//         _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
//             .Do(call =>
//             {
//                 callCount++;
//                 if (callCount == 2)
//                 {
//                     throw new IOException("Partial write failure");
//                 }
//                 call.Arg<Stream>().Write(new byte[100]);
//             });
//
//         var writer = CreateWriter(segment);
//         var batch = CreateTestBatch();
//
//         // Act
//         await writer.AppendAsync(batch); // First succeeds
//         var act = async () => await writer.AppendAsync(batch); // Second fails
//
//         // Assert
//         await act.Should().ThrowAsync<IOException>()
//             .WithMessage("*Partial write failure*");
//     }
//
//     [Fact]
//     public async Task DisposeAsync_Should_Handle_Multiple_Dispose_Calls()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         var writer = CreateWriter(segment);
//
//         // Act
//         await writer.DisposeAsync();
//         var act = async () => await writer.DisposeAsync();
//
//         // Assert - Should not throw
//         await act.Should().NotThrowAsync();
//     }
//
//     [Fact]
//     public async Task Flush_Should_Handle_IOException()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         var writer = CreateWriter(segment);
//         
//         // Create a batch that will write successfully
//         _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
//             .Do(call => call.Arg<Stream>().Write(new byte[10]));
//
//         var batch = CreateTestBatch();
//         await writer.AppendAsync(batch);
//
//         // Now make the internal stream invalid by deleting the file
//         // This will cause Flush to fail on next dispose
//         File.Delete(segment.LogPath);
//
//         // Act
//         var act = async () => await writer.DisposeAsync();
//
//         // Assert - Should propagate the flush error
//         await act.Should().ThrowAsync<Exception>();
//     }
//
//     [Fact]
//     public void Constructor_Should_Handle_Readonly_Directory()
//     {
//         if (OperatingSystem.IsWindows())
//         {
//             // This test is primarily for Unix-like systems
//             return;
//         }
//
//         // Arrange
//         var readonlyDir = Path.Combine(_testDirectory, "readonly");
//         Directory.CreateDirectory(readonlyDir);
//         
//         // Make directory read-only
//         var dirInfo = new DirectoryInfo(readonlyDir);
//         dirInfo.Attributes |= FileAttributes.ReadOnly;
//
//         try
//         {
//             var segment = new LogSegment(
//                 Path.Combine(readonlyDir, "00000000000000000000.log"),
//                 Path.Combine(readonlyDir, "00000000000000000000.index"),
//                 Path.Combine(readonlyDir, "00000000000000000000.timeindex"),
//                 0,
//                 0
//             );
//
//             // Act
//             var act = () => new BinaryLogSegmentWriter(
//                 _batchWriter,
//                 segment,
//                 1024 * 1024,
//                 1024,
//                 1000,
//                 4096
//             );
//
//             // Assert
//             act.Should().Throw<UnauthorizedAccessException>()
//                 .Or.Throw<IOException>();
//         }
//         finally
//         {
//             // Cleanup
//             dirInfo.Attributes &= ~FileAttributes.ReadOnly;
//         }
//     }
//
//     [Fact]
//     public async Task AppendAsync_Should_Maintain_Consistency_After_Error()
//     {
//         // Arrange
//         var segment = CreateSegment();
//         var callCount = 0;
//         
//         _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
//             .Do(call =>
//             {
//                 callCount++;
//                 if (callCount == 2)
//                 {
//                     throw new IOException("Simulated error");
//                 }
//                 call.Arg<Stream>().Write(new byte[100]);
//             });
//
//         var writer = CreateWriter(segment);
//         var batch = CreateTestBatch();
//
//         // Act
//         await writer.AppendAsync(batch); // First succeeds
//         try
//         {
//             await writer.AppendAsync(batch); // Second fails
//         }
//         catch (IOException)
//         {
//             // Expected
//         }
//
//         // Assert - State should still be consistent
//         // (This tests that the writer doesn't get into an invalid state after error)
//         writer.ShouldRoll().Should().BeFalse();
//     }
//
//     private LogSegment CreateSegment(ulong baseOffset = 0)
//     {
//         var logPath = Path.Combine(_testDirectory, $"{baseOffset:D20}.log");
//         var indexPath = Path.Combine(_testDirectory, $"{baseOffset:D20}.index");
//         var timeIndexPath = Path.Combine(_testDirectory, $"{baseOffset:D20}.timeindex");
//         
//         return new LogSegment(logPath, indexPath, timeIndexPath, baseOffset, 0);
//     }
//
//     private BinaryLogSegmentWriter CreateWriter(
//         LogSegment segment,
//         ulong maxSegmentBytes = 1024 * 1024,
//         uint indexIntervalBytes = 1024,
//         uint timeIndexIntervalMs = 1000,
//         uint fileBufferSize = 4096)
//     {
//         return new BinaryLogSegmentWriter(
//             _batchWriter,
//             segment,
//             maxSegmentBytes,
//             indexIntervalBytes,
//             timeIndexIntervalMs,
//             fileBufferSize
//         );
//     }
//
//     private LogRecordBatch CreateTestBatch(ulong baseOffset = 0, int recordCount = 1)
//     {
//         var records = new List<LogRecord>();
//         for (int i = 0; i < recordCount; i++)
//         {
//             records.Add(new LogRecord(baseOffset + (ulong)i, 1000 + (ulong)i, new byte[] { (byte)i }));
//         }
//         
//         return new LogRecordBatch(
//             CommitLogMagicNumbers.LogRecordBatchMagicNumber,
//             baseOffset,
//             records,
//             false
//         );
//     }
//
//     public void Dispose()
//     {
//         if (Directory.Exists(_testDirectory))
//         {
//             try
//             {
//                 // Reset any read-only attributes
//                 foreach (var dir in Directory.GetDirectories(_testDirectory))
//                 {
//                     var dirInfo = new DirectoryInfo(dir);
//                     dirInfo.Attributes &= ~FileAttributes.ReadOnly;
//                 }
//                 
//                 Directory.Delete(_testDirectory, true);
//             }
//             catch
//             {
//                 // Best effort cleanup
//             }
//         }
//     }
// }
//
//


