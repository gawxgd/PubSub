using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SchemaRegistry.Infrastructure.Validation;
using Microsoft.Extensions.Configuration;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Infrastructure.Adapter;

var builder = WebApplication.CreateBuilder(args);

// Configuration already loads appsettings.json + env vars

// DI registrations
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Read config and register store (for now File)
var cfg = builder.Configuration.GetSection("SchemaRegistry");
var storageType = cfg.GetValue<string>("StorageType") ?? "File";

if (storageType.Equals("File", StringComparison.OrdinalIgnoreCase))
{
    builder.Services.AddSingleton<ISchemaStore>(sp =>
    {
        var folder = builder.Configuration.GetValue<string>("SchemaRegistry:FileStore:FolderPath") ?? "data/schemas";
        return new FileSchemaStore(folder);
    });
}
else
{
    throw new InvalidOperationException("Only File store implemented in this demo.");
}

// core services
builder.Services.AddSingleton<ICompatibilityChecker, CompatibilityChecker>();
builder.Services.AddScoped<ISchemaRegistryService, SchemaRegistryService>();
builder.Services.AddScoped<ISchemaCompatibilityService, SchemaCompatibilityService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseRouting();
app.UseAuthorization();
app.MapControllers();

app.Run();