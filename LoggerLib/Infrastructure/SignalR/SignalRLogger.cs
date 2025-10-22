using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using Microsoft.AspNetCore.SignalR;

namespace LoggerLib.Infrastructure.SignalR
{
    /// <summary>
    /// A logger implementation that broadcasts log messages to all connected SignalR clients.
    /// </summary>
    public class SignalRLogger(IHubContext<LogHub> hubContext) : ILogger
    {
        /// <summary>
        /// Sends an informational log message to all clients.
        /// </summary>
        /// <param name="source">The source of the log (e.g., TcpServer, MessageBroker).</param>
        /// <param name="message">The log message content.</param>
        public void LogInfo(LogSource source, string message)
        {
            hubContext.Clients.All.SendAsync("ReceiveLog", LogType.INFO, source.ToString(), message);
        }

        /// <summary>
        /// Sends an error log message to all clients.
        /// </summary>
        /// <param name="source">The source of the log.</param>
        /// <param name="message">The error message content.</param>
        public void LogError(LogSource source, string message)
        {
            hubContext.Clients.All.SendAsync("ReceiveLog", LogType.ERROR, source.ToString(), message);
        }

        /// <summary>
        /// Sends a debug log message to all clients.
        /// </summary>
        /// <param name="source">The source of the log.</param>
        /// <param name="message">The debug message content.</param>
        public void LogDebug(LogSource source, string message)
        {
            hubContext.Clients.All.SendAsync("ReceiveLog", LogType.DEBUG, source.ToString(), message);
        }

        /// <summary>
        /// Sends a warning log message to all clients.
        /// </summary>
        /// <param name="source">The source of the log.</param>
        /// <param name="message">The warning message content.</param>
        public void LogWarning(LogSource source, string message)
        {
            hubContext.Clients.All.SendAsync("ReceiveLog", LogType.WARNING, source.ToString(), message);
        }
    }
}
