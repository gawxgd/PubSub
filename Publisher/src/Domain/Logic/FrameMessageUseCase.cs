using System.IO.Pipelines;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;

namespace Publisher.Domain.Logic;

public class FrameMessageUseCase(IMessageFramer messageFramer)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<FrameMessageUseCase>(LogSource.Publisher);

    private readonly MessageWithTopicFormatter _formatter = new();

    public async Task WriteFramedMessageAsync(
        PipeWriter writer,
        string topic,
        byte[] batchBytes,
        CancellationToken cancellationToken)
    {
        var messageWithTopic = new MessageWithTopic(topic, batchBytes);
        var formattedMessage = _formatter.Format(messageWithTopic);

        var framedMessage = messageFramer.FrameMessage(formattedMessage);
        await writer.WriteAsync(framedMessage, cancellationToken);

        Logger.LogDebug(
            $"Framed message: topic='{topic}', totalLength={formattedMessage.Length}, batchLength={batchBytes.Length}");
    }
}
