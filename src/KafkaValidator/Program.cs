using System.Text.Json;
using KafkaFlow;
using KafkaValidator.Configuration;
using KafkaValidator.Kafka;
using KafkaValidator.Metrics;
using KafkaValidator.Validation;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<KafkaValidatorOptions>()
    .BindConfiguration(KafkaValidatorOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value);

builder.Services.AddSingleton<KafkaValidatorMetrics>();
builder.Services.AddSingleton<RunRegistry>();
builder.Services.AddSingleton<ValidatorEngine>();
builder.Services.AddSingleton<ValidationHandler>();
builder.Services.AddSingleton<ValidationService>();

builder.Services.AddHttpClient<WriterClient>((sp, http) =>
{
    var cfg = sp.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value;
    http.BaseAddress = new Uri(cfg.Writer.BaseUrl.TrimEnd('/') + "/");
    http.Timeout = TimeSpan.FromSeconds(5);
});
builder.Services.AddHttpClient<CdcClient>();

// KafkaFlow — consumer topic outbox.events (requirements.md 15). Ключевая
// семантика: группа kafka-validator + Earliest + WithoutStoringOffsets —
// оффсеты НИКОГДА не коммитятся (в KafkaFlow это NullOffsetCommitter/
// NullOffsetManager), поэтому каждый старт процесса перематывает topic с
// начала и полностью восстанавливает счётчики всех прогонов. Закоммиченные
// оффсеты после рестарта «съели» бы историю — и валидатор ложно сообщил бы
// о потерях. Побочный эффект: kafka_consumergroup_lag для этой группы не
// возникает (нет закоммиченных оффсетов) — §13.3 «если применимо»,
// см. docs/deviations.md 8.
var kafkaBrokers = (builder.Configuration["KafkaValidator:Kafka:Brokers"] ?? "kafka1:9092,kafka2:9092,kafka3:9092")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var kafkaTopic = builder.Configuration["KafkaValidator:Kafka:Topic"] ?? "outbox.events";
var kafkaGroupId = builder.Configuration["KafkaValidator:Kafka:GroupId"] ?? "kafka-validator";
var bufferSize = int.TryParse(builder.Configuration["KafkaValidator:Kafka:BufferSize"], out var bs) ? Math.Clamp(bs, 100, 100_000) : 1000;
var workersCount = int.TryParse(builder.Configuration["KafkaValidator:Kafka:WorkersCount"], out var wc) ? Math.Clamp(wc, 1, 16) : 1;

builder.Services.AddKafkaFlowHostedService(kafka => kafka
    .UseConsoleLog()
    .AddCluster(cluster => cluster
        .WithBrokers(kafkaBrokers)
        .AddConsumer(consumer => consumer
            .Topic(kafkaTopic)
            .WithGroupId(kafkaGroupId)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithoutStoringOffsets()
            .WithBufferSize(bufferSize)
            .WithWorkersCount(workersCount)
            .AddMiddlewares(m => m
                .AddDeserializer<PassthroughDeserializer>()
                .AddTypedHandlers(h => h.AddHandler<ValidationHandler>())))));

// ПОСЛЕ AddKafkaFlowHostedService: hosted-сервисы стартуют в порядке
// регистрации — running выставляется, когда шина (и консюмер) уже запущены
builder.Services.AddHostedService<ValidatorLifecycleHostedService>();

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(KafkaValidatorMetrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());

var app = builder.Build();

var cfg = app.Services.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value;
var metrics = app.Services.GetRequiredService<KafkaValidatorMetrics>();
var registry = app.Services.GetRequiredService<RunRegistry>();
metrics.RunsSeenProvider = () => registry.RunsSeen;

app.Logger.LogInformation(
    "KafkaValidator запускается; brokers: {Brokers}; topic: {Topic} группа: {GroupId} (без сохранения оффсетов — старт всегда с начала topic); " +
    "Writer: {WriterUrl} (fetchFromWriter), CDC: {CdcPollingUrl}/{CdcStreamingUrl}",
    cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupId, cfg.Writer.BaseUrl, cfg.CdcPolling.BaseUrl, cfg.CdcStreaming.BaseUrl);

// ---------- Валидация (requirements.md 15) ----------

app.MapGet("/api/validation/state",
    (ValidatorEngine engine, RunRegistry runRegistry, IOptions<KafkaValidatorOptions> options)
        => Results.Ok(engine.GetSnapshot(runRegistry, options.Value)));

app.MapGet("/api/validation/runs",
    (RunRegistry runRegistry) => Results.Ok(runRegistry.ListRuns()));

app.MapGet("/api/validation/runs/{runId:guid}",
    (Guid runId, RunRegistry runRegistry) =>
        runRegistry.GetRun(runId) is { } summary ? Results.Ok(summary) : Results.NotFound(new { runId }));

app.MapPost("/api/validation/validate", async (HttpRequest http, ValidationService service) =>
{
    ValidateRequest? request;
    try
    {
        request = await http.ReadFromJsonAsync<ValidateRequest>();
    }
    catch (JsonException ex)
    {
        return Results.BadRequest(new { errors = new[] { $"некорректный JSON: {ex.Message}" } });
    }
    if (request?.RunId is null)
        return Results.BadRequest(new { errors = new[] { "runId обязателен" } });

    try
    {
        return Results.Ok(await service.ValidateAsync(request, http.HttpContext.RequestAborted));
    }
    catch (WriterUnavailableException ex)
    {
        return Results.Problem(title: "Writer недоступен", detail: ex.Message, statusCode: 502);
    }
    catch (WriterRunMismatchException ex)
    {
        return Results.Problem(title: "runId не совпадает с последним прогоном Writer", detail: ex.Message, statusCode: 409);
    }
    catch (InvalidOperationException ex)
    {
        return Results.BadRequest(new { errors = new[] { ex.Message } });
    }
});

app.MapGet("/health", () => Results.Ok(new { status = "Healthy" }));

// ready = KafkaFlow-шина запущена (консюмер группы уже читает topic с начала);
// пустой topic — тоже ready: сверять пока нечего, но сервис работоспособен
app.MapGet("/ready", (ValidatorEngine engine) => engine.State == "running"
    ? Results.Ok(new { status = "Ready" })
    : Results.Problem(title: "Not Ready", detail: $"состояние: {engine.State}", statusCode: 503));

app.MapPrometheusScrapingEndpoint(); // /metrics — pull для Prometheus

await app.RunAsync();
