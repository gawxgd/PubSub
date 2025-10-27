using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedMessageUseCase
{
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<ProcessReceivedMessageUseCase>(LogSource.MessageBroker);
    
    public async Task ProcessMessageAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        Logger.LogDebug($"Processing message {message.Length} bytes");
        await Task.CompletedTask;
    }
}