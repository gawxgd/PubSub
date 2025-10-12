using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;

namespace MessageBroker.TcpServer;

public class CreateSocketUseCase(IOptionsMonitor<ServerOptions> monitor)
{
    public Socket CreateSocket()
    {
        var options = monitor.CurrentValue;
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        if (options.InlineCompletions)
            Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", "1");

        socket.Bind(new IPEndPoint(IPAddress.Any, options.Port));
        socket.Listen(options.Backlog);

        Console.WriteLine($"Created socket with options: {options}");

        return socket;
    }
}