using Microsoft.Extensions.Options;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Infrastructure.Adapter;
using SchemaRegistry.Infrastructure.Validation;

namespace SchemaRegistry;

public static class ServiceRegistration
{
    public static IServiceCollection AddSchemaRegistryServices(this IServiceCollection services, IConfiguration configuration)
    {
        services
            .AddOptions<SchemaRegistryOptions>()
            .Bind(configuration.GetSection("SchemaRegistry"))
            .ValidateOnStart();

        services.AddSingleton<IValidateOptions<SchemaRegistryOptions>, SchemaRegistryOptionsValidator>();

        services.AddSingleton<ISchemaStore>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<SchemaRegistryOptions>>().Value;
            
            return options.StorageType switch
            {
                "Sqlite" => new SqliteSchemaStore(
                    options.ConnectionString 
                    ?? throw new InvalidOperationException(
                        "ConnectionString is required for Sqlite storage")),

                "File" => new FileSchemaStore(
                    options.FileStoreFolderPath ?? throw new InvalidOperationException(
                        "Folder path is required for file storage")),

                _ => throw new InvalidOperationException(
                    $"Unknown SchemaRegistry storage type: {options.StorageType}")
            };
        });

        services.AddSingleton<ICompatibilityChecker, CompatibilityChecker>();
        services.AddSingleton<ISchemaCompatibilityService, SchemaCompatibilityService>();
        services.AddScoped<ISchemaRegistryService, SchemaRegistryService>();

        return services;
    }
}