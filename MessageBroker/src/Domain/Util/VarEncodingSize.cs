namespace MessageBroker.Domain.Util;

public static class VarEncodingSize
{
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
