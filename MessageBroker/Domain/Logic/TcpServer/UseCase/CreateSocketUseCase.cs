using System.Net;
using System.Net.Sockets;
using LoggerLib;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Options;
using ILogger = LoggerLib.ILogger;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class CreateSocketUseCase(IOptionsMonitor<TcpServerOptions> monitor, ILogger logger)
{
    public Socket CreateSocket()
    {
        var options = monitor.CurrentValue;

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        if (options.InlineCompletions)
            Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", "1");

        socket.Bind(new IPEndPoint(IPAddress.Any, options.Port));
        socket.Listen(options.Backlog);

        logger.LogInfo(LogSource.TcpServer,$"Created socket with options: {options}");

        return socket;
    }
}