using Microsoft.AspNetCore.SignalR;

namespace LoggerLib
{
    public class SignalRLogger : ILogger
    {
        private readonly IHubContext<LogHub> _hubContext;
        
        public SignalRLogger(IHubContext<LogHub> hubContext)
        {
            _hubContext = hubContext;
        }

        public void LogInfo(LogSource source, string message)
        {
            _hubContext.Clients.All.SendAsync("ReceiveLog", "INFO", source.ToString(), message);
        }

        public void LogError(LogSource source, string message)
        {
            _hubContext.Clients.All.SendAsync("ReceiveLog", "ERROR", source.ToString(), message);
        }

        public void LogDebug(LogSource source, string message)
        {
            _hubContext.Clients.All.SendAsync("ReceiveLog", "DEBUG", source.ToString(), message);
        }

        public void LogWarning(LogSource source, string message)
        {
            _hubContext.Clients.All.SendAsync("ReceiveLog", "WARNING", source.ToString(), message);
        }
    }
    
    
}