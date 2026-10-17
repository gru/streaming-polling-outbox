using System.Text.Json;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenTelemetry.Metrics;
using Writer.Configuration;
using Writer.Generation;
using Writer.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<WriterOptions>()
    .BindConfiguration(WriterOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<WriterOptions>>().Value);

builder.Services.AddSingleton<WriterMetrics>();
builder.Services.AddSingleton<OutboxWriter>();
builder.Services.AddSingleton<GenerationEngine>();
// старт в Idle (requirements.md 6.1); StopAsync — graceful shutdown активного прогона
builder.Services.AddHostedService(sp => sp.GetRequiredService<GenerationEngine>());

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(WriterMetrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());

var app = builder.Build();

app.Logger.LogInformation("Writer запускается; режим подключения по умолчанию: {Mode}",
    app.Configuration["Writer:Connection:Mode"] ?? "pgbouncer");

// ---------- Управление генерацией (requirements.md 6.1) ----------

app.MapPost("/api/generation/start", async (HttpRequest http, IOptions<WriterOptions> options, GenerationEngine engine) =>
{
    RunSpec? spec = null;
    if (http.ContentLength is > 0)
    {
        try { spec = await http.ReadFromJsonAsync<RunSpec>(); }
        catch (JsonException) { return Results.BadRequest(new { error = "тело запроса — невалидный JSON" }); }
    }

    var errors = new List<string>();
    var profile = LoadProfile.Resolve(options.Value, spec, errors);
    if (profile is null)
        return Results.BadRequest(new { errors });

    var (ok, error) = engine.Start(profile);
    return ok
        ? Results.Accepted("/api/generation/state", new { runId = profile.RunId, profile.ConnectionMode, profile })
        : Results.Conflict(new { error });
});

app.MapPost("/api/generation/stop", async (GenerationEngine engine) =>
{
    if (!engine.IsRunning)
        return Results.Conflict(new { error = $"генерация не выполняется (state={engine.State})" });
    var results = await engine.StopAsync(TimeSpan.FromSeconds(60));
    return Results.Ok(results ?? (object)new { state = engine.State });
});

app.MapGet("/api/generation/state", (GenerationEngine engine) => Results.Ok(engine.GetState()));

app.MapGet("/api/generation/results", (GenerationEngine engine) =>
    engine.LastResults is { } r ? Results.Ok(r) : Results.NotFound(new { error = "завершённых прогонов ещё нет" }));

app.MapGet("/api/generation/results/ids", (GenerationEngine engine) =>
    engine.GetLastIds() is { } ids ? Results.Ok(ids) : Results.NotFound(new { error = "завершённых прогонов ещё нет" }));

// ---------- Health / ready / metrics ----------

app.MapGet("/health", () => Results.Ok(new { status = "Healthy" }));

// ready = процесс жив и может писать в БД в активном режиме подключения;
// SELECT 1 не требует привилегий на таблицы — годится для sys_writer
app.MapGet("/ready", async (OutboxWriter db, IOptions<WriterOptions> options) =>
{
    var mode = options.Value.Connection.Mode;
    try
    {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var source = db.SourceFor(mode);
        await using var conn = await source.OpenConnectionAsync(timeoutCts.Token);
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        await cmd.ExecuteScalarAsync(timeoutCts.Token);
        return Results.Ok(new { status = "Ready", connectionMode = mode });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Not Ready", detail: ex.Message, statusCode: 503);
    }
});

app.MapPrometheusScrapingEndpoint(); // /metrics — pull для Prometheus

await app.RunAsync();
