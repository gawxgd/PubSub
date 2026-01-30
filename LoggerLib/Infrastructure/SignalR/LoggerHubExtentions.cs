using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.SignalR;

namespace LoggerLib.Infrastructure.SignalR;

public static class LoggerEndpointExtensions
{
    public static IEndpointRouteBuilder MapLogger(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapHub<LogHub>("/loggerhub");
        return endpoints;
    }
}
