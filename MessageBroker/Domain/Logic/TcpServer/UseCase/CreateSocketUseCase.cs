using System.Net;
using System.Net.Sockets;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Options;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class CreateSocketUseCase(IOptionsMonitor<TcpServerOptions> monitor)
{
    public Socket CreateSocket()
    {
        var options = monitor.CurrentValue;

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        if (options.InlineCompletions)
            Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", "1");

        var address = IPAddress.Parse(options.Address);
        socket.Bind(new IPEndPoint(address, options.Port));
        socket.Listen(options.Backlog);

        Console.WriteLine($"Created socket with options: {options}");

        return socket;
    }
}