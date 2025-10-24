namespace MessageBroker.Domain.Util;

public static class BinaryReaderExtensions
{
    /// <summary>
    ///     Reads an unsigned variable-length 32-bit integer (VarUInt) from the stream.
    /// </summary>
    public static uint ReadVarUInt(this BinaryReader br)
    {
        uint result = 0;
        var shift = 0;
        byte b;

        do
        {
            b = br.ReadByte();
            result |= (uint)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return result;
    }

    /// <summary>
    ///     Reads an unsigned variable-length 64-bit integer (VarULong) from the stream.
    /// </summary>
    public static ulong ReadVarULong(this BinaryReader br)
    {
        ulong result = 0;
        var shift = 0;
        byte b;

        do
        {
            b = br.ReadByte();
            result |= (ulong)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return result;
    }
}