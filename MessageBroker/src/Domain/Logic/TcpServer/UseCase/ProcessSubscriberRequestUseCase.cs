using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class ProcessSubscriberRequestUseCase(Socket socket, ICommitLogFactory commitLogFactory)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessSubscriberRequestUseCase>(LogSource.MessageBroker);

    public async Task ProcessRequestAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
    {
        // TODO read topic and offset from message
        var topic = "default";
        ulong offset = 0;

        Logger.LogDebug($"Processing subscriber request: {message.Length} bytes");
        var commitLogReader = commitLogFactory.GetReader(topic);

        var pipe = new Pipe();

        var recordBatch = commitLogReader.ReadRecordBatch(offset);

        //toDo serialize the batch here and send it to the subscriber
    }
}