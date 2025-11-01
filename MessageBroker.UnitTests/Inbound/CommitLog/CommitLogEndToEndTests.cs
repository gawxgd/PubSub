// using System.Text;
// using FluentAssertions;
// using LoggerLib.Domain.Port;
// using LoggerLib.Outbound.Adapter;
// using MessageBroker.Domain.Entities.CommitLog;
// using MessageBroker.Domain.Port.CommitLog.Compressor;
// using MessageBroker.Domain.Port.CommitLog.Record;
// using MessageBroker.Domain.Port.CommitLog.RecordBatch;
// using MessageBroker.Inbound.CommitLog;
// using MessageBroker.Inbound.CommitLog.BatchRecord;
// using MessageBroker.Inbound.CommitLog.Compressor;
// using MessageBroker.Inbound.CommitLog.Record;
// using MessageBroker.Inbound.CommitLog.Segment;
// using NSubstitute;
// using Xunit;
//
// namespace MessageBroker.UnitTests.Inbound.CommitLog;
//
// /// <summary>
// /// End-to-end integration tests that verify the complete write + read flow
// /// using real file I/O and all concrete implementations.
// /// </summary>
// public class CommitLogEndToEndTests : IDisposable
// {
//     private readonly string _testDirectory;
//     private readonly ILogRecordWriter _recordWriter;
//     private readonly ILogRecordReader _recordReader;
//     private readonly ILogRecordBatchWriter _batchWriter;
//     private readonly ILogRecordBatchReader _batchReader;
//     private readonly ICompressor _compressor;
//     private readonly Encoding _encoding;
//
//     public CommitLogEndToEndTests()
//     {
//         var logger = Substitute.For<ILogger>();
//         AutoLoggerFactory.Initialize(logger);
//         
//         _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
//         Directory.CreateDirectory(_testDirectory);
//         
//         _recordWriter = new LogRecordBinaryWriter();
//         _recordReader = new LogRecordBinaryReader();
//         _compressor = new NoopCompressor();
//         _encoding = Encoding.UTF8;
//         _batchWriter = new LogRecordBatchBinaryWriter(_recordWriter, _compressor, _encoding);
//         _batchReader = new LogRecordBatchBinaryReader(_recordReader, _compressor, _encoding);
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Write_And_Read_Single_Message()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory();
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         var payload = "Hello World"u8.ToArray();
//
//         // Act - Write
//         await appender.AppendAsync(payload);
//         await Task.Delay(100); // Wait for flush
//         await appender.DisposeAsync();
//
//         // Act - Read
//         var logPath = Path.Combine(_testDirectory, "00000000000000000000.log");
//         File.Exists(logPath).Should().BeTrue();
//         
//         using var stream = File.OpenRead(logPath);
//         var batch = _batchReader.ReadBatch(stream);
//
//         // Assert
//         batch.Records.Should().HaveCount(1);
//         batch.Records.First().Payload.ToArray().Should().BeEquivalentTo(payload);
//         batch.Records.First().Offset.Should().Be(0);
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Write_And_Read_Multiple_Messages()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory();
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         var messages = new[]
//         {
//             "Message 1"u8.ToArray(),
//             "Message 2"u8.ToArray(),
//             "Message 3"u8.ToArray(),
//             "Message 4"u8.ToArray(),
//             "Message 5"u8.ToArray()
//         };
//
//         // Act - Write
//         foreach (var msg in messages)
//         {
//             await appender.AppendAsync(msg);
//         }
//         await Task.Delay(100); // Wait for flush
//         await appender.DisposeAsync();
//
//         // Act - Read
//         var logPath = Path.Combine(_testDirectory, "00000000000000000000.log");
//         using var stream = File.OpenRead(logPath);
//         var batch = _batchReader.ReadBatch(stream);
//
//         // Assert
//         batch.Records.Should().HaveCount(5);
//         for (int i = 0; i < messages.Length; i++)
//         {
//             batch.Records.ElementAt(i).Payload.ToArray().Should().BeEquivalentTo(messages[i]);
//             batch.Records.ElementAt(i).Offset.Should().Be((ulong)i);
//         }
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Create_Index_Files()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory(indexIntervalBytes: 100);
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         // Act - Write enough data to trigger index creation
//         for (int i = 0; i < 20; i++)
//         {
//             await appender.AppendAsync(new byte[50]); // 50 bytes each
//         }
//         await Task.Delay(150);
//         await appender.DisposeAsync();
//
//         // Assert
//         var indexPath = Path.Combine(_testDirectory, "00000000000000000000.index");
//         var timeIndexPath = Path.Combine(_testDirectory, "00000000000000000000.timeindex");
//         
//         File.Exists(indexPath).Should().BeTrue();
//         File.Exists(timeIndexPath).Should().BeTrue();
//         
//         new FileInfo(indexPath).Length.Should().BeGreaterThan(0);
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Handle_Segment_Rolling()
//     {
//         // Arrange - Small segment size to force rolling
//         var factory = CreateSegmentFactory(maxSegmentBytes: 500);
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         // Act - Write enough data to trigger roll
//         for (int i = 0; i < 30; i++)
//         {
//             await appender.AppendAsync(new byte[100]);
//         }
//         await Task.Delay(200);
//         await appender.DisposeAsync();
//
//         // Assert - Multiple segment files should exist
//         var logFiles = Directory.GetFiles(_testDirectory, "*.log");
//         logFiles.Length.Should().BeGreaterThan(1, "should have rolled to new segment");
//         
//         // Verify we can read from all segments
//         var totalRecords = 0;
//         foreach (var logFile in logFiles.OrderBy(f => f))
//         {
//             using var stream = File.OpenRead(logFile);
//             var batch = _batchReader.ReadBatch(stream);
//             totalRecords += batch.Records.Count;
//         }
//         
//         totalRecords.Should().Be(30);
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Maintain_Offset_Continuity_Across_Segments()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory(maxSegmentBytes: 300);
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         // Act - Write to force multiple segments
//         for (int i = 0; i < 20; i++)
//         {
//             await appender.AppendAsync(new byte[50]);
//         }
//         await Task.Delay(200);
//         await appender.DisposeAsync();
//
//         // Assert - Read all segments and verify offset continuity
//         var allOffsets = new List<ulong>();
//         var logFiles = Directory.GetFiles(_testDirectory, "*.log").OrderBy(f => f).ToList();
//         
//         foreach (var logFile in logFiles)
//         {
//             using var stream = File.OpenRead(logFile);
//             var batch = _batchReader.ReadBatch(stream);
//             allOffsets.AddRange(batch.Records.Select(r => r.Offset));
//         }
//
//         allOffsets.Should().HaveCount(20);
//         allOffsets.Should().BeInAscendingOrder();
//         allOffsets.Should().OnlyHaveUniqueItems();
//         
//         // Verify sequential
//         for (int i = 0; i < allOffsets.Count; i++)
//         {
//             allOffsets[i].Should().Be((ulong)i);
//         }
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Preserve_Timestamps()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory();
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         var beforeWrite = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
//
//         // Act - Write
//         await appender.AppendAsync("test"u8.ToArray());
//         await Task.Delay(100);
//         await appender.DisposeAsync();
//
//         var afterWrite = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
//
//         // Act - Read
//         var logPath = Path.Combine(_testDirectory, "00000000000000000000.log");
//         using var stream = File.OpenRead(logPath);
//         var batch = _batchReader.ReadBatch(stream);
//
//         // Assert
//         var timestamp = batch.Records.First().Timestamp;
//         timestamp.Should().BeGreaterOrEqualTo((ulong)beforeWrite);
//         timestamp.Should().BeLessOrEqualTo((ulong)afterWrite);
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Handle_Large_Batch()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory();
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(100),
//             "test-topic"
//         );
//
//         var messageCount = 1000;
//
//         // Act - Write many messages
//         for (int i = 0; i < messageCount; i++)
//         {
//             await appender.AppendAsync(BitConverter.GetBytes(i));
//         }
//         await Task.Delay(200);
//         await appender.DisposeAsync();
//
//         // Act - Read
//         var logPath = Path.Combine(_testDirectory, "00000000000000000000.log");
//         using var stream = File.OpenRead(logPath);
//         var batch = _batchReader.ReadBatch(stream);
//
//         // Assert
//         batch.Records.Should().HaveCount(messageCount);
//         
//         for (int i = 0; i < messageCount; i++)
//         {
//             var value = BitConverter.ToInt32(batch.Records.ElementAt(i).Payload.ToArray());
//             value.Should().Be(i);
//         }
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Handle_Various_Payload_Sizes()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory();
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         var payloads = new[]
//         {
//             new byte[0],              // Empty
//             new byte[1],              // Tiny
//             new byte[100],            // Small
//             new byte[1000],           // Medium
//             new byte[10000]           // Large
//         };
//
//         // Fill with recognizable patterns
//         for (int i = 0; i < payloads.Length; i++)
//         {
//             Array.Fill(payloads[i], (byte)(i + 1));
//         }
//
//         // Act - Write
//         foreach (var payload in payloads)
//         {
//             await appender.AppendAsync(payload);
//         }
//         await Task.Delay(100);
//         await appender.DisposeAsync();
//
//         // Act - Read
//         var logPath = Path.Combine(_testDirectory, "00000000000000000000.log");
//         using var stream = File.OpenRead(logPath);
//         var batch = _batchReader.ReadBatch(stream);
//
//         // Assert
//         batch.Records.Should().HaveCount(payloads.Length);
//         
//         for (int i = 0; i < payloads.Length; i++)
//         {
//             var record = batch.Records.ElementAt(i);
//             record.Payload.Length.Should().Be(payloads[i].Length);
//             
//             if (payloads[i].Length > 0)
//             {
//                 record.Payload.ToArray().Should().AllSatisfy(b => b.Should().Be((byte)(i + 1)));
//             }
//         }
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Write_Valid_Index_Entries()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory(indexIntervalBytes: 50);
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         // Act - Write enough to trigger multiple index entries
//         for (int i = 0; i < 20; i++)
//         {
//             await appender.AppendAsync(new byte[30]);
//         }
//         await Task.Delay(150);
//         await appender.DisposeAsync();
//
//         // Assert
//         var indexPath = Path.Combine(_testDirectory, "00000000000000000000.index");
//         File.Exists(indexPath).Should().BeTrue();
//         
//         var indexData = await File.ReadAllBytesAsync(indexPath);
//         indexData.Length.Should().BeGreaterThan(0);
//         indexData.Length.Should().Be(indexData.Length / 16 * 16, 
//             "index file should contain complete 16-byte entries");
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Handle_Rapid_Sequential_Writes()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory();
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(200),
//             "test-topic"
//         );
//
//         var messageCount = 100;
//
//         // Act - Write as fast as possible
//         var tasks = new List<Task>();
//         for (int i = 0; i < messageCount; i++)
//         {
//             tasks.Add(appender.AppendAsync(BitConverter.GetBytes(i)).AsTask());
//         }
//         await Task.WhenAll(tasks);
//         await Task.Delay(300);
//         await appender.DisposeAsync();
//
//         // Act - Read
//         var logPath = Path.Combine(_testDirectory, "00000000000000000000.log");
//         using var stream = File.OpenRead(logPath);
//         var batch = _batchReader.ReadBatch(stream);
//
//         // Assert
//         batch.Records.Should().HaveCount(messageCount);
//         var offsets = batch.Records.Select(r => r.Offset).ToList();
//         offsets.Should().OnlyHaveUniqueItems();
//         offsets.Should().BeInAscendingOrder();
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Create_Proper_Segment_Filenames()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory(maxSegmentBytes: 200);
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromMilliseconds(50),
//             "test-topic"
//         );
//
//         // Act - Force multiple segment rolls
//         for (int i = 0; i < 30; i++)
//         {
//             await appender.AppendAsync(new byte[50]);
//         }
//         await Task.Delay(300);
//         await appender.DisposeAsync();
//
//         // Assert
//         var logFiles = Directory.GetFiles(_testDirectory, "*.log")
//             .Select(Path.GetFileName)
//             .OrderBy(f => f)
//             .ToList();
//
//         logFiles.Should().NotBeEmpty();
//         
//         // First segment should always be 00000000000000000000
//         logFiles.First().Should().Be("00000000000000000000.log");
//         
//         // All segments should have proper 20-digit format
//         foreach (var file in logFiles)
//         {
//             file.Should().MatchRegex(@"^\d{20}\.log$");
//         }
//     }
//
//     [Fact]
//     public async Task EndToEnd_Should_Flush_On_Dispose()
//     {
//         // Arrange
//         var factory = CreateSegmentFactory();
//         var appender = new BinaryCommitLogAppender(
//             factory,
//             _testDirectory,
//             0,
//             TimeSpan.FromHours(1), // Very long flush interval
//             "test-topic"
//         );
//
//         // Act - Write and dispose immediately (before flush interval)
//         await appender.AppendAsync("test"u8.ToArray());
//         await appender.DisposeAsync(); // Should flush remaining data
//
//         // Assert - Data should be written despite not waiting for flush interval
//         var logPath = Path.Combine(_testDirectory, "00000000000000000000.log");
//         File.Exists(logPath).Should().BeTrue();
//         
//         using var stream = File.OpenRead(logPath);
//         var batch = _batchReader.ReadBatch(stream);
//         
//         batch.Records.Should().HaveCount(1);
//         batch.Records.First().Payload.ToArray().Should().BeEquivalentTo("test"u8.ToArray());
//     }
//
//     private BinaryLogSegmentFactory CreateSegmentFactory(
//         ulong maxSegmentBytes = 10 * 1024 * 1024,
//         uint indexIntervalBytes = 4096,
//         uint timeIndexIntervalMs = 1000,
//         uint fileBufferSize = 4096)
//     {
//         return new BinaryLogSegmentFactory(
//             _batchWriter,
//             maxSegmentBytes,
//             indexIntervalBytes,
//             timeIndexIntervalMs,
//             fileBufferSize
//         );
//     }
//
//     public void Dispose()
//     {
//         if (Directory.Exists(_testDirectory))
//         {
//             try
//             {
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
//


