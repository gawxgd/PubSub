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
using MessageBroker.Inbound.Adapter;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Index.Reader;
using MessageBroker.Inbound.CommitLog.Index.Writer;
using MessageBroker.Inbound.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.Segment;
using MessageBroker.Inbound.TcpServer.Service;
using MessageBroker.Infrastructure.Configuration.Options;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using Microsoft.Extensions.Options;

namespace MessageBroker.Infrastructure.DependencyInjection;

public static class ServiceRegistration
{
    /// <summary>
    ///     Register all tcp services
    /// </summary>
    public static void AddTcpServices(this IServiceCollection services)
    {
        // Connection management
        services.AddSingleton<IConnectionRepository, InMemoryConnectionRepository>();
        services.AddSingleton<IConnectionManager, ConnectionManager>();
        // TCP server
        services.AddSingleton<CreateSocketUseCase>();
        services.AddHostedService<TcpServer>();
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
        services.AddSingleton<ITopicSegmentManagerRegistry, TopicSegmentManagerRegistry>();
        services.AddSingleton<ICommitLogFactory, CommitLogFactory>();
        services.AddSingleton<IOffsetIndexWriter, BinaryOffsetIndexWriter>();
        services.AddSingleton<IOffsetIndexReader, BinaryOffsetIndexReader>();
        services.AddSingleton<ITimeIndexWriter, BinaryTimeIndexWriter>();
        services.AddSingleton<ITimeIndexReader, BinaryTimeIndexReader>();
    }

    /// <summary>
    ///     Register all options / configuration objects
    /// </summary>
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