namespace MessageBroker.Domain.Util;

public static class BinaryWriterExtensions
{
    /// <summary>
    /// Writes an unsigned variable-length 32-bit integer (VarUInt) to the stream.
    /// </summary>
    public static void WriteVarUInt(this BinaryWriter bw, uint value)
    {
        while ((value & 0xFFFFFF80U) != 0)
        {
            bw.Write((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }

        bw.Write((byte)value);
    }

    /// <summary>
    /// Writes an unsigned variable-length 64-bit integer (VarULong) to the stream.
    /// </summary>
    public static void WriteVarULong(this BinaryWriter bw, ulong value)
    {
        while ((value & 0xFFFFFFFFFFFFFF80UL) != 0)
        {
            bw.Write((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }

        bw.Write((byte)value);
    }
}