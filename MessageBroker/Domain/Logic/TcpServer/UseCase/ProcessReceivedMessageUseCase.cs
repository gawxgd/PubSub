using Microsoft.Extensions.Logging;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedMessageUseCase(ILogger<ProcessReceivedMessageUseCase> logger)
{
    public async Task ProcessMessageAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processing message {Length} bytes", message.Length);
    }
}