using System.IO.Pipelines;
using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class SendPublishResponseUseCase(IMessageFramer messageFramer)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<SendPublishResponseUseCase>(LogSource.MessageBroker);

    private readonly PublishResponseFormatter _formatter = new();

    public async Task SendResponseAsync(Socket socket, PublishResponse response, CancellationToken cancellationToken)
    {
        var formattedResponse = _formatter.Format(response);
        var framedResponse = messageFramer.FrameMessage(formattedResponse);

        await socket.SendAsync(framedResponse, SocketFlags.None, cancellationToken);

        Logger.LogDebug($"Sent produce response: baseOffset={response.BaseOffset}, errorCode={response.ErrorCode}");
    }
}

