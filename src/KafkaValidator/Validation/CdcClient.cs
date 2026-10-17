using System.Text.Json;

namespace KafkaValidator.Validation;

/// <summary>
/// Обогащение вердикта снапшотом CDC-сервиса (§15: «количество сообщений,
/// подтверждённых CDC» — из фактических данных API, не из Prometheus).
/// Профили polling/streaming взаимоисключающие (requirements.md 11):
/// отвечает максимум один; ни один (CDC остановлен) → null.
/// </summary>
public sealed class CdcClient
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly HttpClient _http;
    private readonly Configuration.KafkaValidatorOptions _options;

    public CdcClient(HttpClient http, Configuration.KafkaValidatorOptions options)
    {
        _http = http;
        _options = options;
    }

    public async Task<ValidationVerdict.CdcSnapshot?> FetchAsync(CancellationToken ct)
    {
        foreach (var (arm, baseUrl) in new[]
                 {
                     ("polling", _options.CdcPolling.BaseUrl),
                     ("streaming", _options.CdcStreaming.BaseUrl),
                 })
        {
            if (string.IsNullOrWhiteSpace(baseUrl))
                continue;
            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(2));
                using var response = await _http.GetAsync($"{baseUrl.TrimEnd('/')}/api/cdc/state", timeoutCts.Token);
                if (!response.IsSuccessStatusCode)
                    continue;
                var state = await response.Content.ReadFromJsonAsync<JsonElement>(JsonOptions, timeoutCts.Token);
                return new ValidationVerdict.CdcSnapshot
                {
                    Arm = arm,
                    State = state.TryGetProperty("state", out var s) ? s.GetString() ?? "" : "",
                    MessagesDelivered = state.TryGetProperty("messagesDelivered", out var d) && d.TryGetInt64(out var delivered) ? delivered : 0,
                    ConfirmedLsn = state.TryGetProperty("confirmedLsn", out var l) ? l.GetString() : null,
                    KafkaErrors = state.TryGetProperty("kafkaErrors", out var k) && k.TryGetInt64(out var ke) ? ke : 0,
                    ReadErrors = state.TryGetProperty("readErrors", out var r) && r.TryGetInt64(out var re) ? re : 0,
                };
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // таймаут 2с: CDC этого arm не отвечает (остановлен) — пробуем следующий
            }
            catch (HttpRequestException)
            {
                // контейнер остановленного профиля не резолвится/не отвечает — ожидаемо
            }
        }
        return null;
    }
}
