using System.Buffers.Binary;
using System.Text;

namespace CdcCommon.PgOutput;

/// <summary>
/// Читатель бинарных сообщений pgoutput: все целые — big-endian, строки —
/// NUL-terminated UTF-8 (cstring). Выходит за границу буфера — исключение разбора.
/// </summary>
internal ref struct PgOutputReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _pos;

    public PgOutputReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _pos = 0;
    }

    public int Position => _pos;
    public int Remaining => _buffer.Length - _pos;

    public byte ReadByte()
    {
        Ensure(1, "byte");
        return _buffer[_pos++];
    }

    public short ReadInt16()
    {
        Ensure(2, "int16");
        var v = BinaryPrimitives.ReadInt16BigEndian(_buffer[_pos..]);
        _pos += 2;
        return v;
    }

    public int ReadInt32()
    {
        Ensure(4, "int32");
        var v = BinaryPrimitives.ReadInt32BigEndian(_buffer[_pos..]);
        _pos += 4;
        return v;
    }

    public long ReadInt64()
    {
        Ensure(8, "int64");
        var v = BinaryPrimitives.ReadInt64BigEndian(_buffer[_pos..]);
        _pos += 8;
        return v;
    }

    public string ReadCString()
    {
        var rest = _buffer[_pos..];
        var end = rest.IndexOf((byte)0);
        if (end < 0)
            throw new PgOutputParseException($"cstring без завершающего NUL на смещении {_pos}");
        var s = Encoding.UTF8.GetString(rest[..end]);
        _pos += end + 1;
        return s;
    }

    public byte[] ReadBytes(int count)
    {
        Ensure(count, "байты");
        var bytes = _buffer.Slice(_pos, count).ToArray();
        _pos += count;
        return bytes;
    }

    /// <summary>Пропуск TupleData целиком (int16 ncols + значения) — для Delete-записей.</summary>
    public void SkipTupleData()
    {
        var ncols = ReadInt16();
        for (var i = 0; i < ncols; i++)
        {
            var kind = ReadByte();
            switch (kind)
            {
                case (byte)'t':
                    var len = ReadInt32();
                    if (len < 0)
                        throw new PgOutputParseException($"отрицательная длина значения колонки ({len})");
                    Ensure(len, "значение колонки");
                    _pos += len;
                    break;
                case (byte)'u': // unchanged TOAST
                case (byte)'n': // NULL
                    break;
                default:
                    throw new PgOutputParseException($"неизвестный вид значения колонки '{(char)kind}'");
            }
        }
    }

    private void Ensure(int count, string what)
    {
        if (_pos + count > _buffer.Length)
            throw new PgOutputParseException(
                $"запись обрывается: нужно {what} ({count} байт) на смещении {_pos}, всего {_buffer.Length}");
    }
}
