using Microsoft.AspNetCore.SignalR;

namespace LoggerLib.Infrastructure.SignalR
{
    /// <summary>
    /// SignalR hub used for broadcasting log messages to connected clients.
    /// </summary>
    public class LogHub : Hub
    {
        // This class can be extended to handle client-to-server interactions if needed.
        // For now, it serves as a broadcast channel for server-side logging via SignalR.
    }
}