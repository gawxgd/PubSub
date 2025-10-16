using Logger.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddLoggerCore();

var app = builder.Build();

app.UseCors("AllowFrontendAndServices");

app.MapLogEndpoints();

app.Run();