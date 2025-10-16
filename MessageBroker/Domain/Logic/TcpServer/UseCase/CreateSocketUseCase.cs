using System.Net;
using System.Net.Sockets;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class CreateSocketUseCase(IOptionsMonitor<TcpServerOptions> monitor, ILogger<CreateSocketUseCase> logger)
{
    public Socket CreateSocket()
    {
        var options = monitor.CurrentValue;

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        if (options.InlineCompletions)
            Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", "1");

        socket.Bind(new IPEndPoint(IPAddress.Any, options.Port));
        socket.Listen(options.Backlog);

        logger.LogInformation("Created socket with options: {@Options}", options);

        return socket;
    }
}