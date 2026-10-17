using System.Text.Json;

namespace KafkaValidator.Validation;

/// <summary>
/// Извлечение тестового идентификатора прогона из body записи (requirements.md 15:
/// «уникальный идентификатор тестового прогона … в body»). Это конвенция
/// тестового контура (Writer кладёт runId в body), а не часть конверта 4.2.
/// </summary>
public static class BodyRunId
{
    public static Guid? TryGet(string bodyJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(bodyJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return null;
            if (doc.RootElement.TryGetProperty("runId", out var el) is false || el.ValueKind != JsonValueKind.String)
                return null;
            return Guid.TryParse(el.GetString(), out var runId) ? runId : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
