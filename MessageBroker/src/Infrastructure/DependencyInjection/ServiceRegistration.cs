using System.Text;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;
using MessageBroker.Domain.Port.CommitLog.Index.Writer;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;
using MessageBroker.Inbound.Adapter;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Index.Reader;
using MessageBroker.Inbound.CommitLog.Index.Writer;
using MessageBroker.Inbound.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.Segment;
using MessageBroker.Inbound.CommitLog.TopicSegmentManager;
using MessageBroker.Inbound.TcpServer.Service;
using MessageBroker.Infrastructure.Configuration.Options;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using MessageBroker.Outbound.Adapter;
using MessageBroker.Outbound.TcpServer.Service;

namespace MessageBroker.Infrastructure.DependencyInjection;

public static class ServiceRegistration
{
    public static void AddTcpServices(this IServiceCollection services)
    {
        services.AddSingleton<IMessageFramer, MessageFramer>();
        services.AddSingleton<IMessageDeframer, MessageDeframer>();
        services.AddSingleton<SendPublishResponseUseCase>();

        services.AddSingleton<IConnectionRepository, InMemoryConnectionRepository>();
        services.AddSingleton<ISubscriberDeliveryMetrics, SubscriberDeliveryMetrics>();
        services.AddSingleton<IConnectionManager, ConnectionManager>();
        services.AddSingleton<MessageBroker.Domain.Port.IStatisticsService, MessageBroker.Inbound.Adapter.StatisticsService>();
        services.AddSingleton<CreateSocketUseCase>();
        services.AddHostedService<SubscriberTcpServer>();
        services.AddHostedService<PublisherTcpServer>();
    }

    public static void AddCommitLogServices(this IServiceCollection services)
    {
        services.AddSingleton<Encoding>(_ => Encoding.UTF8);
        services.AddSingleton<ICompressor, NoopCompressor>();
        services.AddSingleton<ILogRecordWriter, LogRecordBinaryWriter>();
        services.AddSingleton<ILogRecordReader, LogRecordBinaryReader>();
        services.AddSingleton<ILogRecordBatchWriter, LogRecordBatchBinaryWriter>();
        services.AddSingleton<ILogRecordBatchReader, LogRecordBatchBinaryReader>();
        services.AddSingleton<ILogSegmentFactory, BinaryLogSegmentFactory>();
        services.AddSingleton<ITopicSegmentRegistryFactory, TopicSegmentRegistryFactory>();
        services.AddSingleton<ICommitLogFactory, CommitLogFactory>();
        services.AddSingleton<IOffsetIndexWriter, BinaryOffsetIndexWriter>();
        services.AddSingleton<IOffsetIndexReader, BinaryOffsetIndexReader>();
        services.AddSingleton<ITimeIndexWriter, BinaryTimeIndexWriter>();
        services.AddSingleton<ITimeIndexReader, BinaryTimeIndexReader>();
    }

    public static void AddBrokerOptions(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<TcpServerOptions>(
            configuration.GetSection("Server")
        );
        services.Configure<CommitLogOptions>(
            configuration.GetSection("CommitLog")
        );
        services.Configure<List<CommitLogTopicOptions>>(
            configuration.GetSection("CommitLogTopics")
        );
    }
}
