using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;

namespace MessageBroker.IntegrationTests;

public static class IntegrationTestHelpers
{

    public static byte[] CreateBatchBytes(params byte[][] payloads)
    {

        var records = new List<LogRecord>();
        var currentTime = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        
        for (int i = 0; i < payloads.Length; i++)
        {
            records.Add(new LogRecord(
                0,
                currentTime + (ulong)i,
                payloads[i]
            ));
        }

        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );

        using var ms = new MemoryStream();
        var recordWriter = new LogRecordBinaryWriter();
        var compressor = new NoopCompressor();
        var encoding = Encoding.UTF8;
        var batchWriter = new LogRecordBatchBinaryWriter(recordWriter, compressor, encoding);
        batchWriter.WriteTo(batch, ms);
        return ms.ToArray();
    }
}
