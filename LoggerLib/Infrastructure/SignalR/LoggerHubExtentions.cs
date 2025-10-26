using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.SignalR;

namespace LoggerLib.Infrastructure.SignalR;

public static class LoggerEndpointExtensions
{
    public static IApplicationBuilder MapLogger(this IApplicationBuilder app)
    {
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapHub<LogHub>("/loggerhub");
        });

        return app;
    }
}