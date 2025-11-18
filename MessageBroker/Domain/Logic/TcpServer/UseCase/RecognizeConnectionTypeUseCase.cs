using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Enums;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class RecognizeConnectionTypeUseCase(ChannelReader<ReadOnlyMemory<byte>> messageChannelReader)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessReceivedPublisherMessageUseCase>(LogSource.MessageBroker);

    public ConnectionType RecognizeConnectionType(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}