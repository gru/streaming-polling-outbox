using System.Buffers.Binary;
using System.Text;
using CdcCommon.PgOutput;

namespace CdcPolling.UnitTests;

/// <summary>
/// Сборка golden-байтов pgoutput (proto_version=1) для тестов парсера:
/// big-endian целые, cstring — NUL-terminated UTF-8.
/// </summary>
internal static class PgOutputTestBuilder
{
    public const int OutboxRelationId = 16385;
    public const int OtherRelationId = 16390;

    public const string DefaultId = "018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a";
    public const string DefaultTopic = "outbox.events";
    public const string DefaultBody = """{"eventType":"TestEvent","runId":"11111111-2222-3333-4444-555555555555","payload":"abc"}""";
    public const string DefaultCreatedAt = "2026-10-16 12:34:56.789+00";

    /// <summary>Колонки public.outbox_messages: id uuid(2950), topic text(25), body jsonb(3802), created_at timestamptz(1184).</summary>
    public static (string Name, uint Oid)[] OutboxColumns() =>
    [
        ("id", 2950), ("topic", 25), ("body", 3802), ("created_at", 1184),
    ];

    public static RawSlotRecord Rec(ulong lsn, uint xid, byte[] data) => new(CdcCommon.Lsn.Parse(FormatLsn(lsn)), xid, data);

    public static string FormatLsn(ulong v) => $"{v >> 32:X}/{v & 0xFFFFFFFFUL:X8}";

    public static byte[] Begin(long finalLsn, long commitTs, int xid)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write((byte)'B');
        w.WriteBE(finalLsn);
        w.WriteBE(commitTs);
        w.WriteBE(xid);
        return ms.ToArray();
    }

    public static byte[] Commit(long commitLsn, long endLsn, long commitTs)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write((byte)'C');
        w.WriteBE(commitLsn);
        w.WriteBE(endLsn);
        w.WriteBE(commitTs);
        return ms.ToArray();
    }

    public static byte[] Relation(int relationId, string schema, string name, params (string Name, uint Oid)[] columns)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write((byte)'R');
        w.WriteBE(relationId);
        WriteCString(w, schema);
        WriteCString(w, name);
        w.Write((byte)'d'); // replica identity (default) — есть в записи pgoutput
        w.WriteBE((short)columns.Length);
        foreach (var (colName, oid) in columns)
        {
            // flags: 1 = колонка входит в replica identity (PK); id — ключ
            w.Write((byte)(colName == "id" ? 1 : 0));
            WriteCString(w, colName);
            w.WriteBE(unchecked((int)oid));
            w.WriteBE(-1);    // typmod
        }
        return ms.ToArray();
    }

    /// <summary>Insert: 'N' + TupleData; null → 'n', "«u»" → 'u' (unchanged TOAST), иначе текст 't'.</summary>
    public static byte[] Insert(int relationId, params string?[] values)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write((byte)'I');
        w.WriteBE(relationId);
        w.Write((byte)'N');
        w.WriteBE((short)values.Length);
        foreach (var v in values)
        {
            if (v is null) w.Write((byte)'n');
            else if (v == "«u»") w.Write((byte)'u');
            else
            {
                w.Write((byte)'t');
                var bytes = Encoding.UTF8.GetBytes(v);
                w.WriteBE(bytes.Length);
                w.Write(bytes);
            }
        }
        return ms.ToArray();
    }

    /// <summary>Delete: 'K' (ключ по replica identity PK) + TupleData только ключевых колонок (остальные 'n').</summary>
    public static byte[] Delete(int relationId, int totalColumns, int keyIndex, string keyValue)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write((byte)'D');
        w.WriteBE(relationId);
        w.Write((byte)'K');
        w.WriteBE((short)totalColumns);
        for (var i = 0; i < totalColumns; i++)
        {
            if (i != keyIndex)
            {
                w.Write((byte)'n');
            }
            else
            {
                w.Write((byte)'t');
                var bytes = Encoding.UTF8.GetBytes(keyValue);
                w.WriteBE(bytes.Length);
                w.Write(bytes);
            }
        }
        return ms.ToArray();
    }

    public static byte[] Unknown(byte kind) => [kind, 0x00, 0x01];

    /// <summary>Усечённый Insert: заголовок обещает колонки, но данных нет.</summary>
    public static byte[] TruncatedInsert(int relationId)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write((byte)'I');
        w.WriteBE(relationId);
        w.Write((byte)'N');
        w.WriteBE((short)2);
        w.Write((byte)'t');
        w.WriteBE(100500); // длина, которой нет в буфере
        return ms.ToArray();
    }

    /// <summary>Транзакция Writer с batch в n строк: B, R, n×Insert, n×Delete, C.</summary>
    public static List<RawSlotRecord> WriterTransaction(ulong beginLsn, uint xid, int rows, string? topic = null)
    {
        var records = new List<RawSlotRecord>();
        ulong lsn = beginLsn;
        records.Add(Rec(++lsn, xid, Begin((long)beginLsn + 1000, 1_760_000_000_000_000, (int)xid)));
        records.Add(Rec(++lsn, xid, Relation(OutboxRelationId, "public", "outbox_messages", OutboxColumns())));
        for (var i = 0; i < rows; i++)
            records.Add(Rec(++lsn, xid, Insert(OutboxRelationId, DefaultId, topic ?? DefaultTopic, DefaultBody, DefaultCreatedAt)));
        for (var i = 0; i < rows; i++)
            records.Add(Rec(++lsn, xid, Delete(OutboxRelationId, 4, 0, DefaultId)));
        var commitLsn = ++lsn;
        records.Add(Rec(commitLsn, xid, Commit((long)commitLsn, (long)commitLsn + 8, 1_760_000_000_000_000)));
        return records;
    }

    private static void WriteCString(BinaryWriter w, string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        w.Write(bytes);
        w.Write((byte)0);
    }

    private static void WriteBE(this BinaryWriter w, short v)
    {
        Span<byte> b = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(b, v);
        w.Write(b);
    }

    private static void WriteBE(this BinaryWriter w, int v)
    {
        Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(b, v);
        w.Write(b);
    }

    private static void WriteBE(this BinaryWriter w, long v)
    {
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(b, v);
        w.Write(b);
    }
}
