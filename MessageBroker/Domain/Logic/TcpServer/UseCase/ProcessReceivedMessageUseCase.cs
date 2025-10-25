using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedMessageUseCase
{
    private readonly IAutoLogger _logger = AutoLoggerFactory.CreateLogger<ProcessReceivedMessageUseCase>(LogSource.MessageBroker);
    
    public async Task ProcessMessageAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        _logger.LogDebug($"Processing message {message.Length} bytes");
        await Task.CompletedTask;
    }
}