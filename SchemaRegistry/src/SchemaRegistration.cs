using Microsoft.Extensions.Options;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Infrastructure.Adapter;
using SchemaRegistry.Infrastructure.Validation;

namespace SchemaRegistry;

public static class ServiceRegistration
{
    public static IServiceCollection AddSchemaRegistryServices(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<SchemaRegistryOptions>(configuration.GetSection("SchemaRegistry"));

        services.AddSingleton<ISchemaStore>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<SchemaRegistryOptions>>().Value;
            return new FileSchemaStore(options.FileStoreFolderPath);
        });

        services.AddSingleton<ICompatibilityChecker, CompatibilityChecker>();
        services.AddSingleton<ISchemaCompatibilityService, SchemaCompatibilityService>();
        services.AddScoped<ISchemaRegistryService, SchemaRegistryService>();

        return services;
    }
}