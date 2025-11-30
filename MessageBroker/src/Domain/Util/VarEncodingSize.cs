namespace MessageBroker.Domain.Util;

public static class VarEncodingSize
{
    /// <summary>
    /// Returns the number of bytes required to serialize a ulong as a VarULong.
    /// </summary>
    public static int GetVarULongSize(ulong value)
    {
        var size = 0;
        do
        {
            size++;
            value >>= 7;
        } while (value != 0);

        return size;
    }

    /// <summary>
    /// Returns the number of bytes required to serialize a uint as a VarUInt.
    /// </summary>
    public static int GetVarUIntSize(uint value)
    {
        var size = 0;
        do
        {
            size++;
            value >>= 7;
        } while (value != 0);

        return size;
    }
}