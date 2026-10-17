using System.Buffers;
using System.Globalization;
using System.Text.Json;

namespace CdcCommon;

/// <summary>
/// Конверт сообщения requirements.md 4.2: {"id","topic","body","timestamp"}.
/// body вставляется как есть (jsonb уже валидный JSON — без экранирования строки),
/// timestamp — UTC created_at в формате ISO-8601 с миллисекундами (по нему
/// считается end-to-end latency).
/// </summary>
public static class Envelope
{
    public static byte[] Serialize(OutboxEvent e)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("id", e.Id);
            writer.WriteString("topic", e.Topic);
            writer.WritePropertyName("body");
            writer.WriteRawValue(e.BodyJson);
            writer.WriteString("timestamp", e.CreatedAtUtc.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture));
            writer.WriteEndObject();
        }
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Разбор конверта, записанного в Kafka (использует KafkaValidator, этап 7;
    /// CDC-сервисы не вызывают). Строгий: все четыре поля обязательны, id —
    /// валидный Guid, timestamp — UTC. Любая структурная проблема → false.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> envelope, out ConsumedMessage? message)
    {
        message = null;
        try
        {
            using var doc = JsonDocument.Parse(new ReadOnlySequence<byte>(envelope.ToArray()));
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return false;
            if (root.TryGetProperty("id", out var idEl) is false || idEl.ValueKind != JsonValueKind.String)
                return false;
            if (Guid.TryParse(idEl.GetString(), out var id) is false)
                return false;
            if (root.TryGetProperty("topic", out var topicEl) is false || topicEl.ValueKind != JsonValueKind.String)
                return false;
            if (root.TryGetProperty("body", out var bodyEl) is false)
                return false;
            if (root.TryGetProperty("timestamp", out var tsEl) is false || tsEl.ValueKind != JsonValueKind.String)
                return false;

            // Serialize пишет "yyyy-MM-dd'T'HH:mm:ss.fff'Z'"; общий фолбэк — на случай
            // чужих/будущих форматов, чтобы валидатор не терял корректные записи
            var tsStyles = DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;
            if (DateTimeOffset.TryParseExact(tsEl.GetString(), "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
                    CultureInfo.InvariantCulture, tsStyles, out var ts) is false
                && DateTimeOffset.TryParse(tsEl.GetString(), CultureInfo.InvariantCulture, tsStyles, out ts) is false)
                return false;

            message = new ConsumedMessage(id, topicEl.GetString()!, bodyEl.GetRawText(), ts);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}

/// <summary>Конверт, разобранный из записи Kafka (без LSN/xid — их в конверте нет).</summary>
public sealed record ConsumedMessage(Guid Id, string Topic, string BodyJson, DateTimeOffset TimestampUtc);
