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
}
