using System.Net.NetworkInformation;

namespace MessageBroker.E2ETests.Infrastructure;

public static class PortManager
{
    private static int _currentPort = 15000;

    public static int GetNextPort()
    {
        var port = Interlocked.Increment(ref _currentPort);
        
        while (!IsPortAvailable(port))
        {
            port = Interlocked.Increment(ref _currentPort);
        }
        
        return port;
    }

    private static bool IsPortAvailable(int port)
    {
        try
        {
            var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
            var tcpConnInfoArray = ipGlobalProperties.GetActiveTcpListeners();
            
            return !tcpConnInfoArray.Any(tcpi => tcpi.Port == port);
        }
        catch
        {
            return true;
        }
    }
}
