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

public class CommitLogPayloadEqualityIntegrationTests : IDisposable
{
    private readonly string _dir;
    private readonly IServiceProvider _sp;

    public CommitLogPayloadEqualityIntegrationTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
        _dir = Path.Combine(Path.GetTempPath(), $"mb_payload_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);

        var services = new ServiceCollection();
        services.AddCommitLogServices();
        services.Configure<CommitLogOptions>(o =>
        {
            o.Directory = _dir;
            o.MaxSegmentBytes = 2_000_000;
            o.IndexIntervalBytes = 128;
            o.TimeIndexIntervalMs = 10;
            o.ReaderLogBufferSize = 64 * 1024;
            o.ReaderIndexBufferSize = 8 * 1024;
            o.FileBufferSize = 4096;
        });
        services.Configure<List<CommitLogTopicOptions>>(o =>
        {
            o.Add(new CommitLogTopicOptions { Name = "payload", BaseOffset = 0, FlushIntervalMs = 10 });
        });
        _sp = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Should_Preserve_Empty_And_Small_Payloads()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var app = factory.GetAppender("payload");
        var empty = Array.Empty<byte>();
        var small = new byte[] { 1, 2, 3, 4 };

        await app.AppendAsync(CreateBatchBytes(empty));
        await app.AppendAsync(CreateBatchBytes(small));
        await Task.Delay(100);

        var reader = factory.GetReader("payload");
        var batch0 = reader.ReadRecordBatch(0)!;
        var batch1 = reader.ReadRecordBatch(1)!;
        batch0.Should().NotBeNull();
        batch1.Should().NotBeNull();
        batch0.Records.First().Payload.ToArray().Should().BeEquivalentTo(empty);
        batch1.Records.First().Payload.ToArray().Should().BeEquivalentTo(small);
        batch0.BaseOffset.Should().Be(0);
        batch1.BaseOffset.Should().Be(1);
    }

    [Fact]
    public async Task Should_Preserve_Large_And_Mixed_Payloads()
    {
        var rnd = new Random(123);
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var app = factory.GetAppender("payload");

        byte[] Large(int size)
        {
            var b = new byte[size];
            rnd.NextBytes(b);
            return b;
        }

        var p1 = Large(1);
        var p2 = Large(10);
        var p3 = Large(10_000);
        var p4 = Large(200_000);
        var p5 = Large(2);

        await app.AppendAsync(CreateBatchBytes(p1));
        await app.AppendAsync(CreateBatchBytes(p2));
        await app.AppendAsync(CreateBatchBytes(p3));
        await app.AppendAsync(CreateBatchBytes(p4));
        await app.AppendAsync(CreateBatchBytes(p5));

        await Task.Delay(300);

        var reader = factory.GetReader("payload");
        var batch0 = reader.ReadRecordBatch(0)!;
        var batch1 = reader.ReadRecordBatch(1)!;
        var batch2 = reader.ReadRecordBatch(2)!;
        var batch3 = reader.ReadRecordBatch(3)!;
        var batch4 = reader.ReadRecordBatch(4)!;

        batch0.Records.First().Payload.ToArray().Should().BeEquivalentTo(p1);
        batch1.Records.First().Payload.ToArray().Should().BeEquivalentTo(p2);
        batch2.Records.First().Payload.ToArray().Should().BeEquivalentTo(p3);
        batch3.Records.First().Payload.ToArray().Should().BeEquivalentTo(p4);
        batch4.Records.First().Payload.ToArray().Should().BeEquivalentTo(p5);
        
        batch0.BaseOffset.Should().Be(0);
        batch1.BaseOffset.Should().Be(1);
        batch2.BaseOffset.Should().Be(2);
        batch3.BaseOffset.Should().Be(3);
        batch4.BaseOffset.Should().Be(4);
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