using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;
using Publisher.Outbound.Exceptions;

namespace Publisher.Domain.Logic;

public sealed class PublishResponseHandler
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<PublishResponseHandler>(LogSource.Publisher);

    public void Handle(PublishResponse response)
    {
        if (response.ErrorCode != ErrorCode.None)
        {
            Logger.LogError($"Broker returned error: errorCode={response.ErrorCode}, baseOffset={response.BaseOffset}");
        }
        else
        {
            Logger.LogDebug($"Received successful response: baseOffset={response.BaseOffset}");
        }
    }
}

