using System.Text;

namespace Writer.Generation;

/// <summary>Сообщение outbox: строка таблицы public.outbox_messages (requirements.md 4.2).</summary>
/// <param name="BodyJson">JSON-тело {"eventType","runId","payload"} — уходит в колонку body (jsonb).</param>
/// <param name="BodyBytes">Размер BodyJson в UTF-8 байтах — база учёта объёма прогона.</param>
public sealed record OutboxMessage(Guid Id, string Topic, string BodyJson, DateTimeOffset CreatedAt, int BodyBytes)
{
    public long BodyBytesLong => BodyBytes;
}

public static class MessageFactory
{
    public const string Topic = "outbox.events";
    public const string EventType = "TestEvent";

    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /// <summary>
    /// id и created_at задаёт клиент: у sys_writer нет SELECT-привилегии, поэтому
    /// INSERT ... RETURNING недоступен, и идентичность UUID при повторе батча
    /// (разрешение неоднозначного commit) обязана обеспечиваться на стороне Writer.
    /// UUIDv7: монотонные по времени идентификаторы лучше локализуют вставки в PK-индексе.
    /// </summary>
    public static OutboxMessage Create(Guid runId, int payloadBytes, DateTimeOffset? createdAt = null)
    {
        payloadBytes = Math.Max(0, payloadBytes);
        var payload = new char[payloadBytes];
        for (var i = 0; i < payloadBytes; i++)
            payload[i] = Alphabet[Random.Shared.Next(Alphabet.Length)];

        // Псевдослучайный alphanumeric payload: 1 символ ASCII = 1 байт UTF-8,
        // размер контролируется точно; неконстантное содержимое не даёт TOAST-компрессии
        // занижать фактический объём jsonb/WAL (важно для WAL-метрик и WAL Pressure Test).
        var body = $"{{\"eventType\":\"{EventType}\",\"runId\":\"{runId}\",\"payload\":\"{new string(payload)}\"}}";
        return new OutboxMessage(CreateUuidV7(), Topic, body, createdAt ?? DateTimeOffset.UtcNow,
            Encoding.UTF8.GetByteCount(body));
    }

    public static (OutboxMessage[] Messages, long TotalBodyBytes) CreateBatch(Guid runId, int count, int payloadBytes)
    {
        var messages = new OutboxMessage[count];
        long bytes = 0;
        for (var i = 0; i < count; i++)
        {
            messages[i] = Create(runId, payloadBytes);
            bytes += messages[i].BodyBytes;
        }
        return (messages, bytes);
    }

    /// <summary>Оценка размера body до генерации (длина payload фиксирована, runId — 36 символов).</summary>
    public static int EstimateBodyBytes(Guid runId, int payloadBytes) =>
        Encoding.UTF8.GetByteCount(
            $"{{\"eventType\":\"{EventType}\",\"runId\":\"{runId}\",\"payload\":\"{new string('x', payloadBytes)}\"}}");

    /// <summary>
    /// UUIDv7 (RFC 9562): 48 бит unix-ms + случайность. Guid.CreateVersion7 появился только в .NET 9.
    /// Поля раскладываются под mixed-endian контракт Guid(ReadOnlySpan&lt;byte&gt;): первые три поля
    /// little-endian, поэтому каноническая строка UUID получает timestamp в старших разрядах.
    /// </summary>
    public static Guid CreateUuidV7()
    {
        var ms = (DateTimeOffset.UtcNow.UtcTicks - 621355968000000000L) / TimeSpan.TicksPerMillisecond;
        Span<byte> b = stackalloc byte[16];
        Random.Shared.NextBytes(b);
        b[0] = (byte)(ms >> 16); // _a (LE): старшие 32 бита 48-битного timestamp
        b[1] = (byte)(ms >> 24);
        b[2] = (byte)(ms >> 32);
        b[3] = (byte)(ms >> 40);
        b[4] = (byte)ms;        // _b (LE): младшие 16 бит timestamp
        b[5] = (byte)(ms >> 8);
        b[6] &= 0x0F;           // rand_a (младшие 12 бит поля _c)
        b[7] = (byte)(0x70 | (b[7] & 0x0F)); // version 7
        b[8] = (byte)(0x80 | (b[8] & 0x3F)); // variant RFC 4122
        return new Guid(b);
    }
}
