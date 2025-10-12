using MessageBroker.Infrastructure;
using Microsoft.Extensions.Hosting;

await Host
    .CreateDefaultBuilder(args)
    .ConfigureMessageBroker(args)
    .Build()
    .RunAsync();