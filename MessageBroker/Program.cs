using MessageBroker.Infrastructure.Configuration;
using Microsoft.Extensions.Hosting;

await Host
    .CreateDefaultBuilder(args)
    .ConfigureMessageBroker(args)
    .Build()
    .RunAsync();