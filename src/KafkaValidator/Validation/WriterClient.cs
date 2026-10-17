using System.Net.Http.Json;
using System.Text.Json;

namespace KafkaValidator.Validation;

/// <summary>
/// Клиент Writer: ожидаемый набор id последнего завершённого прогона
/// (GET /api/generation/results/ids — RunIdsDto создан для KafkaValidator,
/// requirements.md 15). 404 = завершённого прогона нет.
/// </summary>
public sealed class WriterClient
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly HttpClient _http;

    public WriterClient(HttpClient http) => _http = http;

    /// <exception cref="HttpRequestException">Writer недоступен или ответил не 200/404.</exception>
    public async Task<WriterRunIds?> FetchLastRunIdsAsync(CancellationToken ct)
    {
        using var response = await _http.GetAsync("/api/generation/results/ids", ct);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            return null;
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<WriterRunIds>(JsonOptions, ct);
    }
}
