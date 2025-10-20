using LoggerLib;
using LoggerLib.Domain.Enums;
using ILogger = LoggerLib.Domain.Port.ILogger;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedMessageUseCase(ILogger logger)
{
    public async Task ProcessMessageAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        logger.LogInfo(LogSource.TcpServer, $"Processing message {message.Length} bytes");
    }
}