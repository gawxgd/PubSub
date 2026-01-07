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
        if (batchBytes == null || batchBytes.Length == 0)
        {
            Logger.LogError($"❌ Cannot frame empty batch! batchBytes is null or empty");
            throw new InvalidOperationException("Cannot frame empty batch");
        }
        
        var messageWithTopic = new MessageWithTopic(topic, batchBytes);
        var formattedMessage = _formatter.Format(messageWithTopic);

        if (formattedMessage == null || formattedMessage.Length == 0)
        {
            Logger.LogError($"❌ Formatted message is empty! batchBytes.Length={batchBytes.Length}");
            throw new InvalidOperationException("Formatted message is empty");
        }

        var framedMessage = messageFramer.FrameMessage(formattedMessage);
        
        if (framedMessage == null || framedMessage.Length == 0)
        {
            Logger.LogError($"❌ Framed message is empty! formattedMessage.Length={formattedMessage.Length}");
            throw new InvalidOperationException("Framed message is empty");
        }
        
        await writer.WriteAsync(framedMessage, cancellationToken);

        Logger.LogInfo(
            $"✅ Framed message: topic='{topic}', formattedLength={formattedMessage.Length}, batchLength={batchBytes.Length}, framedLength={framedMessage.Length}");
    }
}