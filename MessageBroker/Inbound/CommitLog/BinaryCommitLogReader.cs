// using MessageBroker.Domain.Entities.CommitLog;
// using MessageBroker.Domain.Port.CommitLog;
// using MessageBroker.Domain.Port.CommitLog.Segment;
// ToDo
// namespace MessageBroker.Inbound.CommitLog;
//
// public sealed class BinaryCommitLogReader : ICommitLogReader
// {
//     private readonly ILogSegmentFactory _segmentFactory;
//     private readonly ITopicSegmentManager _segmentManager;
//     private readonly string _topic;
//     private ILogSegmentReader? _segmentReader;
//     private ulong _currentSegmentBaseOffset;
//
//     public BinaryCommitLogReader(ILogSegmentFactory segmentFactory, ITopicSegmentManager segmentManager, string topic)
//     {
//         _segmentFactory = segmentFactory;
//         _segmentManager = segmentManager;
//         _topic = topic;
//         RefreshSegmentReader();
//     }
//
//     private void RefreshSegmentReader()
//     {
//         var activeSegment = _segmentManager.GetActiveSegment();
//
//         if (_segmentReader == null || _currentSegmentBaseOffset != activeSegment.BaseOffset)
//         {
//             _segmentReader?.DisposeAsync().AsTask().Wait();
//             _segmentReader = _segmentFactory.CreateReader(activeSegment);
//             _currentSegmentBaseOffset = activeSegment.BaseOffset;
//         }
//     }
//
//     public LogRecord? ReadRecord(ulong offset)
//     {
//         RefreshSegmentReader();
//         var batch = _segmentReader!.ReadBatch(offset);
//         return batch?.Records.FirstOrDefault(r => r.Offset == offset);
//     }
//
//     public IEnumerable<LogRecord> ReadRecords(ulong startOffset)
//     {
//         RefreshSegmentReader();
//         var batch = _segmentReader!.ReadBatch(startOffset);
//         if (batch == null) yield break;
//         foreach (var record in batch.Records)
//         {
//             if (record.Offset >= startOffset)
//             {
//                 yield return record;
//             }
//         }
//     }
//
//     public IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp)
//     {
//         RefreshSegmentReader();
//         using var enumerator = _segmentReader!.ReadFromTimestamp(timestamp).GetEnumerator();
//         if (!enumerator.MoveNext()) yield break;
//         var batch = enumerator.Current;
//         foreach (var record in batch.Records)
//         {
//             if (record.Timestamp >= timestamp)
//             {
//                 yield return record;
//             }
//         }
//     }
//
//     public ulong GetHighWaterMark()
//     {
//         return _segmentManager.GetHighWaterMark();
//     }
//
//     public async ValueTask DisposeAsync()
//     {
//         if (_segmentReader != null)
//             await _segmentReader.DisposeAsync();
//     }
// }