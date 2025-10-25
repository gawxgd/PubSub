using System.Net;
using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Options;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class CreateSocketUseCase(IOptionsMonitor<TcpServerOptions> monitor)
{
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<CreateSocketUseCase>(LogSource.MessageBroker);

    public Socket CreateSocket()
    {
        var options = monitor.CurrentValue;

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        if (options.InlineCompletions)
        {
            Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", "1");
        }

        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

        var address = IPAddress.Parse(options.Address);
        socket.Bind(new IPEndPoint(address, options.Port));
        socket.Listen(options.Backlog);

        Logger.LogInfo($"Created socket with options: {options}");

        return socket;
    }
}