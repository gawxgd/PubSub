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
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;
using static MessageBroker.IntegrationTests.IntegrationTestHelpers;

namespace MessageBroker.IntegrationTests;

public class CommitLogMultiTopicIntegrationTests : IDisposable
{
    private readonly string _rootDir;
    private readonly IServiceProvider _sp;

    public CommitLogMultiTopicIntegrationTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());

        _rootDir = Path.Combine(Path.GetTempPath(), $"mb_it_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_rootDir);

        var services = new ServiceCollection();

        services.AddCommitLogServices();

        services.Configure<CommitLogOptions>(opts =>
        {
            opts.Directory = _rootDir;
            opts.MaxSegmentBytes = 1024 * 1024;
            opts.IndexIntervalBytes = 128;
            opts.TimeIndexIntervalMs = 50;
            opts.ReaderLogBufferSize = 64 * 1024;
            opts.ReaderIndexBufferSize = 8 * 1024;
            opts.FileBufferSize = 4096;
        });

        services.Configure<List<CommitLogTopicOptions>>(opts =>
        {
            opts.Add(new CommitLogTopicOptions { Name = "topicA", BaseOffset = 0, FlushIntervalMs = 20 });
            opts.Add(new CommitLogTopicOptions { Name = "topicB", BaseOffset = 0, FlushIntervalMs = 20 });
        });

        _sp = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Should_Append_And_Read_From_Multiple_Topics()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();

        var appA = factory.GetAppender("topicA");
        var appB = factory.GetAppender("topicB");

        var A1 = "A1"u8.ToArray();
        var A2 = "A2"u8.ToArray();
        var B1 = "B1"u8.ToArray();
        var B2 = "B2"u8.ToArray();

        await appA.AppendAsync(CreateBatchBytes(A1));
        await appB.AppendAsync(CreateBatchBytes(B1));
        await appA.AppendAsync(CreateBatchBytes(A2));
        await appB.AppendAsync(CreateBatchBytes(B2));

        await Task.Delay(200);

        var readerA = factory.GetReader("topicA");
        var readerB = factory.GetReader("topicB");

        var batchA0 = readerA.ReadRecordBatch(0)!;
        var batchA1 = readerA.ReadRecordBatch(1)!;
        var batchB0 = readerB.ReadRecordBatch(0)!;
        var batchB1 = readerB.ReadRecordBatch(1)!;

        batchA0.Should().NotBeNull();
        batchA1.Should().NotBeNull();
        batchB0.Should().NotBeNull();
        batchB1.Should().NotBeNull();

        batchA0.Records.First().Payload.ToArray().Should().BeEquivalentTo(A1);
        batchA1.Records.First().Payload.ToArray().Should().BeEquivalentTo(A2);
        batchB0.Records.First().Payload.ToArray().Should().BeEquivalentTo(B1);
        batchB1.Records.First().Payload.ToArray().Should().BeEquivalentTo(B2);
    }

    [Fact]
    public async Task Readers_Should_See_Isolated_Offsets_Per_Topic()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var appA = factory.GetAppender("topicA");
        var appB = factory.GetAppender("topicB");

        for (int i = 0; i < 5; i++)
        {
            await appA.AppendAsync(CreateBatchBytes(new byte[] { (byte)(i + 1) }));
            await appB.AppendAsync(CreateBatchBytes(new byte[] { (byte)(i + 11) }));
        }

        await Task.Delay(200);

        var readerA = factory.GetReader("topicA");
        var readerB = factory.GetReader("topicB");

        var batchA0 = readerA.ReadRecordBatch(0)!;
        var batchB0 = readerB.ReadRecordBatch(0)!;

        batchA0.Should().NotBeNull();
        batchB0.Should().NotBeNull();
        batchA0.BaseOffset.Should().Be(0);
        batchB0.BaseOffset.Should().Be(0);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_rootDir)) Directory.Delete(_rootDir, true);
        }
        catch
        {
        }
    }
}