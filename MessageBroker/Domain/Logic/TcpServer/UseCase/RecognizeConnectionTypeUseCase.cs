using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Enums;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class RecognizeConnectionTypeUseCase(ChannelReader<ReadOnlyMemory<byte>> messageChannelReader)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<RecognizeConnectionTypeUseCase>(LogSource.MessageBroker);

    public async Task<ConnectionType> RecognizeConnectionTypeAsync(CancellationToken cancellationToken)
    {
        // Wait for first message to determine connection type
        //TODO think what to do with errors
        if (!await messageChannelReader.WaitToReadAsync(cancellationToken))
        {
            Logger.LogError($"No message received to determine connection type\"");
            throw new InvalidOperationException("No message received to determine connection type");
        }

        if (!messageChannelReader.TryRead(out var firstMessage))
        {
            Logger.LogError($"Failed to read first message");
            throw new InvalidOperationException("Failed to read first message");
        }

        if (firstMessage.Length == 0)
        {
            Logger.LogError($"First message is empty");
            throw new InvalidOperationException("First message is empty");
        }

        // First byte determines connection type:
        // 0x01 = Publisher
        // 0x02 = Subscriber
        var connectionTypeByte = firstMessage.Span[0];
        
        ConnectionType connectionType = connectionTypeByte switch
        {
            0x01 => ConnectionType.Publisher,
            0x02 => ConnectionType.Subscriber,
            _ => throw new InvalidOperationException($"Unknown connection type byte: {connectionTypeByte}")
        };

        Logger.LogInfo($"Recognized connection type: {connectionType} (byte: 0x{connectionTypeByte:X2})");
        return connectionType;
    }
}