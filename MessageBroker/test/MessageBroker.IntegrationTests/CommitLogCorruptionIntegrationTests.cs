using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using MessageBroker.Infrastructure.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;
using static MessageBroker.IntegrationTests.IntegrationTestHelpers;

namespace MessageBroker.IntegrationTests;

public class CommitLogCorruptionIntegrationTests : IDisposable
{
    private readonly string _dir;
    private readonly IServiceProvider _sp;

    public CommitLogCorruptionIntegrationTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
        _dir = Path.Combine(Path.GetTempPath(), $"mb_corr_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);

        var services = new ServiceCollection();
        services.AddCommitLogServices();
        services.Configure<CommitLogOptions>(o =>
        {
            o.Directory = _dir;
            o.MaxSegmentBytes = 1_000_000;
            o.IndexIntervalBytes = 64;
            o.TimeIndexIntervalMs = 10;
            o.ReaderLogBufferSize = 64 * 1024;
            o.ReaderIndexBufferSize = 8 * 1024;
            o.FileBufferSize = 4096;
        });
        services.Configure<List<CommitLogTopicOptions>>(o =>
        {
            o.Add(new CommitLogTopicOptions { Name = "corr", BaseOffset = 0, FlushIntervalMs = 10 });
        });
        _sp = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Reader_Should_Throw_On_Corrupted_Log_Content()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var app = factory.GetAppender("corr");

        for (int i = 0; i < 10; i++)
        {
            await app.AppendAsync(CreateBatchBytes(BitConverter.GetBytes(i)));
        }

        await Task.Delay(200);

        var topicDir = Path.Combine(_dir, "corr");
        var log = Directory.GetFiles(topicDir, "*.log").OrderBy(f => f).First();

        // Corrupt the record data area (which will cause CRC mismatch)
        // Batch structure: baseOffset(8) + batchLength(4) + lastOffset(8) + recordBytesLength(4) + magic(1) + crc(4) + compressed(1) + baseTimestamp(8) + recordBytes...
        // So record bytes start at position: 8 + 4 + 8 + 4 + 1 + 4 + 1 + 8 = 38
        using (var fs = new FileStream(log, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            if (fs.Length > 50)
            {
                fs.Seek(50, SeekOrigin.Begin); // Corrupt somewhere in the record data
                var b = (byte)fs.ReadByte();
                fs.Seek(-1, SeekOrigin.Current);
                fs.WriteByte((byte)(b ^ 0xFF)); // Flip all bits
            }
        }

        var reader = factory.GetReader("corr");
        Action act = () => reader.ReadRecordBatch(0);
        act.Should().Throw<InvalidDataException>("corrupted data should cause CRC mismatch");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
        }
        catch
        {
        }
    }
}