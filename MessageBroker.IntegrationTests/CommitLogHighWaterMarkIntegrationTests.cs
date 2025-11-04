// using System;
// using System.Collections.Generic;
// using System.IO;
// using System.Linq;
// using System.Threading.Tasks;
// using FluentAssertions;
// using LoggerLib.Domain.Port;
// using LoggerLib.Outbound.Adapter;
// using MessageBroker.Domain.Port.CommitLog;
// using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
// using MessageBroker.Infrastructure.DependencyInjection;
// using Microsoft.Extensions.DependencyInjection;
// using NSubstitute;
// using Xunit;
// ToDo
// namespace MessageBroker.IntegrationTests;
//
// public class CommitLogHighWaterMarkIntegrationTests : IDisposable
// {
//     private readonly string _dir;
//     private readonly IServiceProvider _sp;
//
//     public CommitLogHighWaterMarkIntegrationTests()
//     {
//         AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
//         _dir = Path.Combine(Path.GetTempPath(), $"mb_hwm_{Guid.NewGuid():N}");
//         Directory.CreateDirectory(_dir);
//
//         var services = new ServiceCollection();
//         services.AddCommitLogServices();
//         services.Configure<CommitLogOptions>(o =>
//         {
//             o.Directory = _dir;
//             o.MaxSegmentBytes = 1_000_000;
//             o.IndexIntervalBytes = 64;
//             o.TimeIndexIntervalMs = 10;
//             o.ReaderLogBufferSize = 64 * 1024;
//             o.ReaderIndexBufferSize = 8 * 1024;
//             o.FileBufferSize = 4096;
//         });
//         services.Configure<List<CommitLogTopicOptions>>(o =>
//         {
//             o.Add(new CommitLogTopicOptions { Name = "hwm", BaseOffset = 0, FlushIntervalMs = 10 });
//         });
//         _sp = services.BuildServiceProvider();
//     }
//
//     [Fact]
//     public async Task HighWaterMark_Should_Equal_Next_Offset()
//     {
//         var factory = _sp.GetRequiredService<ICommitLogFactory>();
//         var app = factory.GetAppender("hwm");
//         var reader = factory.GetReader("hwm");
//
//         for (int i = 0; i < 1234; i++)
//         {
//             await app.AppendAsync(BitConverter.GetBytes(i));
//         }
//         await Task.Delay(200);
//
//         reader.GetHighWaterMark().Should().Be(1234UL);
//         var recs = reader.ReadRecords(0).ToList();
//         recs.Should().HaveCount(1234);
//         recs.Last().Offset.Should().Be(1233);
//     }
//
//     public void Dispose()
//     {
//         try { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); } catch { }
//     }
// }
//
//


