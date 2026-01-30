namespace MessageBroker.Domain.Util;

public static class BinaryWriterExtensions
{

    public static void WriteVarUInt(this BinaryWriter bw, uint value)
    {
        while ((value & 0xFFFFFF80U) != 0)
        {
            bw.Write((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }

        bw.Write((byte)value);
    }

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
