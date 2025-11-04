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
// public class CommitLogConcurrentReadWriteIntegrationTests : IDisposable
// {
//     private readonly string _dir;
//     private readonly IServiceProvider _sp;
//
//     public CommitLogConcurrentReadWriteIntegrationTests()
//     {
//         AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
//         _dir = Path.Combine(Path.GetTempPath(), $"mb_crw_{Guid.NewGuid():N}");
//         Directory.CreateDirectory(_dir);
//
//         var services = new ServiceCollection();
//         services.AddCommitLogServices();
//         services.Configure<CommitLogOptions>(o =>
//         {
//             o.Directory = _dir;
//             o.MaxSegmentBytes = 64_000;
//             o.IndexIntervalBytes = 64;
//             o.TimeIndexIntervalMs = 5;
//             o.ReaderLogBufferSize = 64 * 1024;
//             o.ReaderIndexBufferSize = 8 * 1024;
//             o.FileBufferSize = 4096;
//         });
//         services.Configure<List<CommitLogTopicOptions>>(o =>
//         {
//             o.Add(new CommitLogTopicOptions { Name = "crw", BaseOffset = 0, FlushIntervalMs = 10 });
//         });
//         _sp = services.BuildServiceProvider();
//     }
//
//     [Fact]
//     public async Task Readers_Should_Observe_Appends_During_And_After_Writes()
//     {
//         var factory = _sp.GetRequiredService<ICommitLogFactory>();
//         var app = factory.GetAppender("crw");
//         var reader1 = factory.GetReader("crw");
//         var reader2 = factory.GetReader("crw");
//
//         var writerTask = Task.Run(async () =>
//         {
//             for (int i = 0; i < 1000; i++)
//             {
//                 await app.AppendAsync(BitConverter.GetBytes(i));
//                 if (i % 50 == 0) await Task.Delay(1);
//             }
//         });
//
//         var observedDuring = new List<ulong>();
//         var readerTask = Task.Run(async () =>
//         {
//             var seen = new HashSet<ulong>();
//             var start = DateTime.UtcNow;
//             while ((DateTime.UtcNow - start).TotalMilliseconds < 1500)
//             {
//                 foreach (var r in reader1.ReadRecords(0))
//                 {
//                     if (seen.Add(r.Offset)) observedDuring.Add(r.Offset);
//                 }
//                 await Task.Delay(10);
//             }
//         });
//
//         await Task.WhenAll(writerTask, readerTask);
//
//         var finalRecs = reader2.ReadRecords(0).ToList();
//         finalRecs.Should().HaveCount(1000);
//         observedDuring.Should().NotBeEmpty();
//         observedDuring.Min().Should().Be(0);
//         observedDuring.Max().Should().BeGreaterThan(100);
//     }
//
//     public void Dispose()
//     {
//         try { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); } catch { }
//     }
// }
//
//


