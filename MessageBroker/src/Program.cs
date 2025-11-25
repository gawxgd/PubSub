using LoggerLib.Outbound.Adapter;
using MessageBroker.Infrastructure.Configuration;
using ILogger = LoggerLib.Domain.Port.ILogger;

var host = Host
    .CreateDefaultBuilder(args)
    .ConfigureMessageBroker(args)
    .Build();

var logger = host.Services.GetRequiredService<ILogger>();
AutoLoggerFactory.Initialize(logger);

await host.RunAsync();