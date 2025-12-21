using System.IO;
using System.Net.Sockets;
using System.Text;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessReceivedPublisherMessageUseCase(
    ICommitLogFactory commitLogFactory) : IMessageProcessorUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedPublisherMessageUseCase>(LogSource.MessageBroker);

    public async Task ProcessAsync(ReadOnlyMemory<byte> message, Socket socket, CancellationToken cancellationToken)
    {
        Logger.LogDebug($"Processing publisher message: {message.Length} bytes");

        try
        {
            //ToDo change to append bytes with correct offset without reading and writing
            var (topic, batchBytes) = ParseMessage(message);
            
            Logger.LogDebug($"Parsed message for topic '{topic}', batch size: {batchBytes.Length} bytes");

            var commitLogAppender = commitLogFactory.GetAppender(topic);
            await commitLogAppender.AppendAsync(batchBytes);

            Logger.LogInfo($"Appended batch ({batchBytes.Length} bytes) to commit log for topic '{topic}'");
        }
        catch (InvalidDataException ex)
        {
            Logger.LogError($"Failed to parse message: {ex.Message}", ex);
            throw;
        }
    }

    private static (string topic, byte[] batchBytes) ParseMessage(ReadOnlyMemory<byte> message)
    {
        var span = message.Span;
        
        // Find the separator ':'
        var separatorIndex = span.IndexOf((byte)':');
        if (separatorIndex == -1)
        {
            throw new InvalidDataException("Invalid publisher message format: missing topic separator");
        }

        var topic = Encoding.UTF8.GetString(span.Slice(0, separatorIndex));
        var batchBytes = span.Slice(separatorIndex + 1).ToArray();

        return (topic, batchBytes);
    }
}
