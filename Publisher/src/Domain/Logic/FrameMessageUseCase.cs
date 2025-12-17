using System.IO.Pipelines;
using System.Text;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace Publisher.Domain.Logic;

/// <summary>
/// Frames messages with length prefix and topic prefix for publisher-broker protocol.
/// Format: [4-byte length][topic][:][batch bytes]
/// </summary>
public class FrameMessageUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<FrameMessageUseCase>(LogSource.Publisher);

    public async Task WriteFramedMessageAsync(
        PipeWriter writer,
        string topic,
        byte[] batchBytes,
        CancellationToken cancellationToken)
    {
        var topicBytes = Encoding.UTF8.GetBytes(topic);
        var separatorByte = (byte)':';
        var totalPayloadLength = topicBytes.Length + 1 + batchBytes.Length; 
        var lengthPrefix = BitConverter.GetBytes(totalPayloadLength);

        await writer.WriteAsync(lengthPrefix, cancellationToken);
        await writer.WriteAsync(topicBytes, cancellationToken);
        await writer.WriteAsync(new[] { separatorByte }, cancellationToken);
        await writer.WriteAsync(batchBytes, cancellationToken);

        Logger.LogDebug($"Framed message: topic='{topic}', totalLength={totalPayloadLength}, batchLength={batchBytes.Length}");
    }
}

