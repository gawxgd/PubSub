using System.Net.Sockets;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedPublisherMessageUseCase(ICommitLogFactory commitLogFactory, string topic) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedPublisherMessageUseCase>(LogSource.MessageBroker);

    private readonly ICommitLogAppender _commitLogAppender = commitLogFactory.GetAppender(topic);

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        Logger.LogDebug($"Processing message {message.Length} bytes");
        
        await _commitLogAppender.AppendAsync(message);
        Logger.LogInfo("Appended message to commit log");
    }
}