using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Infrastructure.SignalR;
using Microsoft.AspNetCore.SignalR;

namespace LoggerLib.Outbound.Adapter;

public class SignalRLogger(IHubContext<LogHub> hubContext) : ILogger
{
    public void LogInfo(LogSource source, string message)
    {
        hubContext.Clients.All.SendAsync("ReceiveLog", nameof(LogType.Info), source.ToString(), message);
    }

    public void LogError(LogSource source, string message)
    {
        hubContext.Clients.All.SendAsync("ReceiveLog", nameof(LogType.Error), source.ToString(), message);
    }

    public void LogDebug(LogSource source, string message)
    {
        hubContext.Clients.All.SendAsync("ReceiveLog", nameof(LogType.Debug), source.ToString(), message);
    }

    public void LogWarning(LogSource source, string message)
    {
        hubContext.Clients.All.SendAsync("ReceiveLog", nameof(LogType.Warning), source.ToString(), message);
    }

    public void LogTrace(LogSource source, string message)
    {
        hubContext.Clients.All.SendAsync("ReceiveLog", nameof(LogType.Trace), source.ToString(), message);
    }
}
