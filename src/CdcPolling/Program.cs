using Confluent.Kafka;
using CdcPolling.Configuration;
using CdcPolling.Kafka;
using CdcPolling.Metrics;
using CdcPolling.Polling;
using KafkaFlow;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<CdcPollingOptions>()
    .BindConfiguration(CdcPollingOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<CdcPollingOptions>>().Value);

builder.Services.AddSingleton<CdcPollingMetrics>();
builder.Services.AddSingleton<SlotGateway>();
builder.Services.AddSingleton<KafkaPublisher>();

// KafkaFlow — основной API публикации (requirements.md 9). acks=all и
// enable.idempotence=true заданы явно; topic НЕ создаётся автоматически
// (requirements.md 3.2: topic создаёт kafka-init).
var kafkaBrokers = (builder.Configuration["CdcPolling:Kafka:Brokers"] ?? "kafka1:9092,kafka2:9092,kafka3:9092")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var kafkaTopic = builder.Configuration["CdcPolling:Kafka:Topic"] ?? "outbox.events";
var kafkaLingerMs = double.TryParse(builder.Configuration["CdcPolling:Kafka:LingerMs"], out var linger) ? linger : 5.0;
var kafkaMessageTimeoutMs = int.TryParse(builder.Configuration["CdcPolling:Kafka:MessageTimeoutMs"], out var timeoutMs) ? timeoutMs : 15_000;

builder.Services.AddKafkaFlowHostedService(kafka => kafka
    .UseConsoleLog()
    .AddCluster(cluster => cluster
        .WithBrokers(kafkaBrokers)
        .AddProducer(KafkaPublisher.ProducerName, producer => producer
            .DefaultTopic(kafkaTopic)
                .WithAcks(KafkaFlow.Acks.All)
            .WithProducerConfig(new ProducerConfig
            {
                EnableIdempotence = true,
                LingerMs = kafkaLingerMs,
                MessageTimeoutMs = kafkaMessageTimeoutMs,
            })
            .AddMiddlewares(m => m.AddSerializer<PassthroughSerializer>()))));

// цикл peek → publish → advance стартует сразу и работает непрерывно
builder.Services.AddSingleton<CdcPollingEngine>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<CdcPollingEngine>());

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(CdcPollingMetrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());

var app = builder.Build();

var cfg = app.Services.GetRequiredService<IOptions<CdcPollingOptions>>().Value;
app.Logger.LogInformation(
    "CDC Polling запускается; режим подключения: {Mode}; brokers: {Brokers}; БД: {Db}@{Host}:{Port} user={User} passSet={PassSet} pollBatch={PollBatch}",
    cfg.Connection.Mode, cfg.Kafka.Brokers, cfg.Connection.Database.Name,
    string.Equals(cfg.Connection.Mode, "direct", StringComparison.OrdinalIgnoreCase)
        ? cfg.Connection.Direct.Host : cfg.Connection.Pgbouncer.Host,
    string.Equals(cfg.Connection.Mode, "direct", StringComparison.OrdinalIgnoreCase)
        ? cfg.Connection.Direct.Port : cfg.Connection.Pgbouncer.Port,
    cfg.Connection.Database.Username,
    !string.IsNullOrEmpty(cfg.Connection.Database.Password),
    cfg.Poll.ClampBatchSize());

// ---------- Наблюдение ----------

app.MapGet("/api/cdc/state", (CdcPollingEngine engine) => Results.Ok(engine.GetSnapshot()));

app.MapGet("/health", () => Results.Ok(new { status = "Healthy" }));

// ready = процесс жив, БД отвечает и слот существует/валиден; при терминальной
// ошибке (error_slot/failed) — 503 с диагностикой
app.MapGet("/ready", async (SlotGateway gateway, IOptions<CdcPollingOptions> options) =>
{
    var mode = options.Value.Connection.Mode;
    try
    {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var status = await gateway.SlotStatusAsync(mode, timeoutCts.Token);
        if (status is null)
            return Results.Problem(title: "Not Ready", detail: "cdc.outbox_slot_status() не вернул данные", statusCode: 503);
        if (!status.Exists)
            return Results.Problem(title: "Not Ready", detail: "слот outbox_slot не существует", statusCode: 503);
        if (status.IsInvalidated)
            return Results.Problem(title: "Not Ready",
                detail: $"слот outbox_slot инвалидирован (wal_status={status.WalStatus}); WAL утрачен",
                statusCode: 503);
        return Results.Ok(new { status = "Ready", connectionMode = mode, walStatus = status.WalStatus });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Not Ready", detail: ex.Message, statusCode: 503);
    }
});

app.MapPrometheusScrapingEndpoint(); // /metrics — pull для Prometheus

await app.RunAsync();
