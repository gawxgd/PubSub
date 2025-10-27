using System.Text;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Inbound.Adapter;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
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
        services.AddSingleton<ILogRecordBatchWriter, LogRecordBatchBinaryWriter>();
        services.AddSingleton<ILogSegmentFactory, BinaryLogSegmentFactory>();

        services.AddSingleton<ICommitLogFactory, CommitLogFactory>();
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