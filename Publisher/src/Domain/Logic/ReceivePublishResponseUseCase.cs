using System.Buffers;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;

namespace Publisher.Domain.Logic;

public class ReceivePublishResponseUseCase(IMessageDeframer messageDeframer)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ReceivePublishResponseUseCase>(LogSource.Publisher);

    private readonly PublishResponseDeformatter _deformatter = new();

    public bool TryReceiveResponse(ref ReadOnlySequence<byte> buffer, out PublishResponse? response)
    {
        response = null;

        if (!messageDeframer.TryReadFramedMessage(ref buffer, out var message))
        {
            return false;
        }

        response = _deformatter.Deformat(message);
        if (response == null)
        {
            Logger.LogWarning("Failed to parse produce response");
            return false;
        }

        if (response.ErrorCode != ErrorCode.None)
        {
            Logger.LogWarning($"Received error response: errorCode={response.ErrorCode}, baseOffset={response.BaseOffset}");
        }
        else
        {
            Logger.LogDebug($"Received produce response: baseOffset={response.BaseOffset}");
        }

        return true;
    }
}

