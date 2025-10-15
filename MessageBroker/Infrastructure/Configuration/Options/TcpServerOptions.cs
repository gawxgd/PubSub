namespace MessageBroker.Infrastructure.Configuration.Options;

public sealed record TcpServerOptions
{
    public int Port { get; init; } = 9096;
    public string Address { get; init; } = "127.0.0.1";
    public int MaxRequestSizeInByte { get; init; } = 512;
    public bool InlineCompletions { get; init; } = false;
    public bool SocketPolling { get; init; } = false;

    public int Backlog { get; init; } = 4096;

    public override string ToString()
    {
        return
            $"ServerOptions: Port={Port}, Address={Address}, MaxRequestSizeInByte={MaxRequestSizeInByte}, InlineCompletions={InlineCompletions}, SocketPolling={SocketPolling}";
    }
}