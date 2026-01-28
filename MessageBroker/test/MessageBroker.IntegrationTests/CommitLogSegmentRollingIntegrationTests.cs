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

public class CommitLogSegmentRollingIntegrationTests : IDisposable
{
    private readonly string _dir;
    private readonly IServiceProvider _sp;

    public CommitLogSegmentRollingIntegrationTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
        _dir = Path.Combine(Path.GetTempPath(), $"mb_roll_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);

        var services = new ServiceCollection();
        services.AddCommitLogServices();
        services.Configure<CommitLogOptions>(o =>
        {
            o.Directory = _dir;
            o.MaxSegmentBytes = 2_000; // very small to force many segments
            o.IndexIntervalBytes = 64;
            o.TimeIndexIntervalMs = 5;
            o.ReaderLogBufferSize = 64 * 1024;
            o.ReaderIndexBufferSize = 8 * 1024;
            o.FileBufferSize = 4096;
        });
        services.Configure<List<CommitLogTopicOptions>>(o =>
        {
            o.Add(new CommitLogTopicOptions { Name = "roll", BaseOffset = 0, FlushIntervalMs = 10 });
        });
        _sp = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Should_Create_Multiple_Segments_And_Preserve_Offset_Continuity()
    {
        //ToDo add method to handle reading many batches
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var app = factory.GetAppender("roll");

        for (int i = 0; i < 1000; i++)
        {
            await app.AppendAsync(CreateBatchBytes(new byte[100]));
        }

        await Task.Delay(500);

        var topicDir = Path.Combine(_dir, "roll");
        var logs = Directory.GetFiles(topicDir, "*.log").OrderBy(f => f).ToList();
        logs.Count.Should().BeGreaterThan(1);

        var reader = factory.GetReader("roll");
        // Read all batches since each AppendAsync creates one batch
        var allOffsets = new List<ulong>();
        for (ulong offset = 0; offset < 1000; offset++)
        {
            var batch = reader.ReadRecordBatch(offset);
            if (batch != null)
            {
                allOffsets.Add(batch.BaseOffset);
            }
        }
        allOffsets.Should().HaveCount(1000);
        allOffsets.Should().OnlyHaveUniqueItems();
        allOffsets.Should().BeInAscendingOrder();
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