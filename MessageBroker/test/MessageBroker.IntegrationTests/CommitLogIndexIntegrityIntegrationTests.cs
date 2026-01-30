using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using MessageBroker.Infrastructure.DependencyInjection;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Index.Reader;
using MessageBroker.Inbound.CommitLog.Record;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;
using static MessageBroker.IntegrationTests.IntegrationTestHelpers;

namespace MessageBroker.IntegrationTests;

public class CommitLogIndexIntegrityIntegrationTests : IDisposable
{
    private readonly string _dir;
    private readonly IServiceProvider _sp;

    public CommitLogIndexIntegrityIntegrationTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
        _dir = Path.Combine(Path.GetTempPath(), $"mb_index_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);

        var services = new ServiceCollection();
        services.AddCommitLogServices();
        services.Configure<CommitLogOptions>(o =>
        {
            o.Directory = _dir;
            o.MaxSegmentBytes = 10_000;
            o.IndexIntervalBytes = 64;
            o.TimeIndexIntervalMs = 10;
            o.ReaderLogBufferSize = 64 * 1024;
            o.ReaderIndexBufferSize = 8 * 1024;
            o.FileBufferSize = 4096;
        });
        services.Configure<List<CommitLogTopicOptions>>(o =>
        {
            o.Add(new CommitLogTopicOptions { Name = "ix", BaseOffset = 0, FlushIntervalMs = 20 });
        });
        _sp = services.BuildServiceProvider();
    }

    [Fact]
    public async Task OffsetIndex_Should_Map_To_Batch_Positions_And_RelativeOffsets()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var app = factory.GetAppender("ix");

        var payload = new byte[50];
        for (int i = 0; i < 10; i++)
        {
            Array.Fill(payload, (byte)(i + 1));
            await app.AppendAsync(CreateBatchBytes(payload));
        }
        await Task.Delay(200);

        var segFiles = Directory.GetFiles(Path.Combine(_dir, "ix"), "*.log");
        segFiles.Should().NotBeEmpty();

        foreach (var logPath in segFiles)
        {
            var baseOffset = ulong.Parse(Path.GetFileNameWithoutExtension(logPath));
            var indexPath = Path.ChangeExtension(logPath, ".index");
            var timeIndexPath = Path.ChangeExtension(logPath, ".timeindex");

            File.Exists(indexPath).Should().BeTrue();
            File.Exists(timeIndexPath).Should().BeTrue();

            using var logStream = File.OpenRead(logPath);
            using var indexStream = File.OpenRead(indexPath);

            var indexReader = new BinaryOffsetIndexReader();
            var recordReader = new LogRecordBinaryReader();
            var batchReader = new LogRecordBatchBinaryReader(recordReader, new MessageBroker.Inbound.CommitLog.Compressor.NoopCompressor(), Encoding.UTF8);

            var entries = ReadAllOffsetIndexEntries(indexStream).ToList();
            entries.Count.Should().BeGreaterThan(0);

            foreach (var entry in entries)
            {
                var pos = (long)entry.FilePosition;
                logStream.Seek(pos, SeekOrigin.Begin);
                var batch = batchReader.ReadBatch(logStream);
                batch.BaseOffset.Should().Be(baseOffset + entry.RelativeOffset);
            }
        }
    }

    [Fact(Skip = "TimeIndex writing is not yet implemented (see TODO in BinaryLogSegmentWriter.cs)")]
    public async Task TimeIndex_Should_Have_Monotonic_Timestamps_And_Valid_RelativeOffsets()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var app = factory.GetAppender("ix");

        for (int i = 0; i < 5; i++)
        {
            await app.AppendAsync(CreateBatchBytes(BitConverter.GetBytes(i)));
            await Task.Delay(15);
        }
        await Task.Delay(200);

        var segFiles = Directory.GetFiles(Path.Combine(_dir, "ix"), "*.log");
        segFiles.Should().NotBeEmpty();

        foreach (var logPath in segFiles)
        {
            var baseOffset = ulong.Parse(Path.GetFileNameWithoutExtension(logPath));
            var timeIndexPath = Path.ChangeExtension(logPath, ".timeindex");
            using var timeIndexStream = File.OpenRead(timeIndexPath);
            var entries = ReadAllTimeIndexEntries(timeIndexStream).ToList();
            entries.Should().NotBeEmpty();
            entries.Select(e => e.Timestamp).Should().BeInAscendingOrder();
        }
    }

    private IEnumerable<OffsetIndexEntry> ReadAllOffsetIndexEntries(Stream indexStream)
    {
        var rdr = new BinaryOffsetIndexReader();
        indexStream.Seek(0, SeekOrigin.Begin);
        while (indexStream.Position < indexStream.Length)
        {
            yield return rdr.ReadFrom(indexStream);
        }
    }

    private IEnumerable<TimeIndexEntry> ReadAllTimeIndexEntries(Stream timeIndexStream)
    {
        var rdr = new BinaryTimeIndexReader();
        timeIndexStream.Seek(0, SeekOrigin.Begin);
        while (timeIndexStream.Position < timeIndexStream.Length)
        {
            yield return rdr.ReadFrom(timeIndexStream);
        }
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); } catch { }
    }
}

