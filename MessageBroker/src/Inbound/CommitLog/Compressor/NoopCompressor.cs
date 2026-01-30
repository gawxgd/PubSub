using MessageBroker.Domain.Port.CommitLog.Compressor;

namespace MessageBroker.Inbound.CommitLog.Compressor;

public class NoopCompressor : ICompressor
{
    public byte[] Compress(byte[] data)
    {
        return data;
    }

    public byte[] Decompress(byte[] data)
    {
        return data;
    }
}
