namespace MessageBroker.E2ETests.Infrastructure;

public static class PortManager
{
    private static int _currentPort = 9100;

    public static int GetNextPort()
    {
        return Interlocked.Increment(ref _currentPort);
    }
}
