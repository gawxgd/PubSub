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

namespace MessageBroker.IntegrationTests;

public class CommitLogEndToEndIntegrationTests : IDisposable
{
    private readonly string _dir;
    private readonly IServiceProvider _sp;

    public CommitLogEndToEndIntegrationTests()
    {
        AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
        _dir = Path.Combine(Path.GetTempPath(), $"mb_e2e_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);

        var services = new ServiceCollection();
        services.AddCommitLogServices();
        services.Configure<CommitLogOptions>(o =>
        {
            o.Directory = _dir;
            o.MaxSegmentBytes = 10_000_000;
            o.IndexIntervalBytes = 64;
            o.TimeIndexIntervalMs = 10;
            o.ReaderLogBufferSize = 64 * 1024;
            o.ReaderIndexBufferSize = 8 * 1024;
            o.FileBufferSize = 4096;
        });
        services.Configure<List<CommitLogTopicOptions>>(o =>
        {
            o.Add(new CommitLogTopicOptions { Name = "default", BaseOffset = 0, FlushIntervalMs = 20 });
        });
        _sp = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Should_Write_And_Read_Single_Message()
    {
        var factory = _sp.GetRequiredService<ICommitLogFactory>();
        var appender = factory.GetAppender("default");

        var payload = "Hello World"u8.ToArray();
        await appender.AppendAsync(payload);
        await Task.Delay(100);

        var reader = factory.GetReader("default");
        var records = reader.ReadRecords(0).ToList();

        records.Should().NotBeEmpty();
        records.First().Payload.ToArray().Should().BeEquivalentTo(payload);
        records.First().Offset.Should().Be(0);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
        }
        catch { }
    }
}


