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
//ToDo
// namespace MessageBroker.IntegrationTests;
//
// public class CommitLogHighLoadIntegrationTests : IDisposable
// {
//     private readonly string _dir;
//     private readonly IServiceProvider _sp;
//
//     public CommitLogHighLoadIntegrationTests()
//     {
//         AutoLoggerFactory.Initialize(Substitute.For<ILogger>());
//         _dir = Path.Combine(Path.GetTempPath(), $"mb_hl_{Guid.NewGuid():N}");
//         Directory.CreateDirectory(_dir);
//
//         var services = new ServiceCollection();
//         services.AddCommitLogServices();
//         services.Configure<CommitLogOptions>(o =>
//         {
//             o.Directory = _dir;
//             o.MaxSegmentBytes = 256_000; // force rolling under load
//             o.IndexIntervalBytes = 128;
//             o.TimeIndexIntervalMs = 5;
//             o.ReaderLogBufferSize = 64 * 1024;
//             o.ReaderIndexBufferSize = 8 * 1024;
//             o.FileBufferSize = 4096;
//         });
//         services.Configure<List<CommitLogTopicOptions>>(o =>
//         {
//             o.Add(new CommitLogTopicOptions { Name = "t1", BaseOffset = 0, FlushIntervalMs = 10 });
//             o.Add(new CommitLogTopicOptions { Name = "t2", BaseOffset = 0, FlushIntervalMs = 10 });
//             o.Add(new CommitLogTopicOptions { Name = "t3", BaseOffset = 0, FlushIntervalMs = 10 });
//         });
//         _sp = services.BuildServiceProvider();
//     }
//
//     [Fact]
//     public async Task Parallel_Appends_Across_Topics_Should_Preserve_Counts_And_Offsets()
//     {
//         var factory = _sp.GetRequiredService<ICommitLogFactory>();
//         var topics = new[] { "t1", "t2", "t3" };
//         var totalPerTopic = 2_000;
//
//         var tasks = new List<Task>();
//         foreach (var t in topics)
//         {
//             var app = factory.GetAppender(t);
//             tasks.Add(Task.Run(async () =>
//             {
//                 for (int i = 0; i < totalPerTopic; i++)
//                 {
//                     await app.AppendAsync(BitConverter.GetBytes(i));
//                 }
//             }));
//         }
//
//         await Task.WhenAll(tasks);
//         await Task.Delay(1000);
//
//         foreach (var t in topics)
//         {
//             var reader = factory.GetReader(t);
//             var recs = reader.ReadRecords(0).ToList();
//             recs.Should().HaveCount(totalPerTopic);
//             recs.Select(r => r.Offset).Should().OnlyHaveUniqueItems();
//             recs.Select(r => r.Offset).Should().BeInAscendingOrder();
//             recs.All(r => r.Timestamp > 0).Should().BeTrue();
//         }
//     }
//
//     public void Dispose()
//     {
//         try { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); } catch { }
//     }
// }
//
//


