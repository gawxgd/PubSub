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

        await appA.AppendAsync("A1"u8.ToArray());
        await appB.AppendAsync("B1"u8.ToArray());
        await appA.AppendAsync("A2"u8.ToArray());
        await appB.AppendAsync("B2"u8.ToArray());

        await Task.Delay(200);

        var readerA = factory.GetReader("topicA");
        var readerB = factory.GetReader("topicB");

        var recordsA = readerA.ReadRecords(0).ToList();
        var recordsB = readerB.ReadRecords(0).ToList();

        recordsA.Should().NotBeEmpty();
        recordsB.Should().NotBeEmpty();

        var payloadsA = recordsA.Select(r => r.Payload.ToArray()).ToList();
        var payloadsB = recordsB.Select(r => r.Payload.ToArray()).ToList();

        var A1 = "A1"u8.ToArray();
        var A2 = "A2"u8.ToArray();
        var B1 = "B1"u8.ToArray();
        var B2 = "B2"u8.ToArray();

        payloadsA.Should().ContainSingle(a => a.SequenceEqual(A1));
        payloadsA.Should().ContainSingle(a => a.SequenceEqual(A2));
        payloadsB.Should().ContainSingle(b => b.SequenceEqual(B1));
        payloadsB.Should().ContainSingle(b => b.SequenceEqual(B2));
    }

    [Fact]
    public async Task Readers_Should_See_Isolated_Offsets_Per_Topic()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var appA = factory.GetAppender("topicA");
        var appB = factory.GetAppender("topicB");

        for (int i = 0; i < 5; i++)
        {
            await appA.AppendAsync(new byte[] { (byte)(i + 1) });
            await appB.AppendAsync(new byte[] { (byte)(i + 11) });
        }

        await Task.Delay(200);

        var readerA = factory.GetReader("topicA");
        var readerB = factory.GetReader("topicB");

        var offsetsA = readerA.ReadRecords(0).Select(r => r.Offset).ToList();
        var offsetsB = readerB.ReadRecords(0).Select(r => r.Offset).ToList();

        offsetsA.Should().NotBeEmpty();
        offsetsB.Should().NotBeEmpty();
        offsetsA.Min().Should().Be(0);
        offsetsB.Min().Should().Be(0);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_rootDir)) Directory.Delete(_rootDir, true);
        }
        catch { }
    }
}


