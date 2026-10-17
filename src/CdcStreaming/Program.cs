using Confluent.Kafka;
using CdcStreaming.Configuration;
using CdcStreaming.Kafka;
using CdcStreaming.Metrics;
using CdcStreaming.Replication;
using CdcStreaming.Streaming;
using KafkaFlow;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<CdcStreamingOptions>()
    .BindConfiguration(CdcStreamingOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<CdcStreamingOptions>>().Value);

builder.Services.AddSingleton<CdcStreamingMetrics>();
builder.Services.AddSingleton<ReplicationGateway>();
builder.Services.AddSingleton<KafkaPublisher>();

// KafkaFlow — основной API публикации (requirements.md 9); topic НЕ создаётся
// автоматически (requirements.md 3.2: topic создаёт kafka-init).
var kafkaBrokers = (builder.Configuration["CdcStreaming:Kafka:Brokers"] ?? "kafka1:9092,kafka2:9092,kafka3:9092")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var kafkaTopic = builder.Configuration["CdcStreaming:Kafka:Topic"] ?? "outbox.events";
var kafkaLingerMs = double.TryParse(builder.Configuration["CdcStreaming:Kafka:LingerMs"], out var linger) ? linger : 5.0;
var kafkaMessageTimeoutMs = int.TryParse(builder.Configuration["CdcStreaming:Kafka:MessageTimeoutMs"], out var timeoutMs) ? timeoutMs : 15_000;

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

// цикл StartReplication → decode → Kafka → feedback стартует сразу и работает непрерывно
builder.Services.AddSingleton<CdcStreamingEngine>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<CdcStreamingEngine>());

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(CdcStreamingMetrics.MeterName)
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());

var app = builder.Build();

var cfg = app.Services.GetRequiredService<IOptions<CdcStreamingOptions>>().Value;
app.Logger.LogInformation(
    "CDC Streaming запускается; replication напрямую через {Host}:{Port} (минуя PgBouncer); brokers: {Brokers}; " +
    "БД: {Db} user={User} passSet={PassSet} slot={Slot} publication={Publication} channelCapacity={ChannelCapacity}",
    cfg.Connection.Endpoint.Host, cfg.Connection.Endpoint.Port, cfg.Kafka.Brokers, cfg.Connection.Database.Name,
    cfg.Connection.Database.Username, !string.IsNullOrEmpty(cfg.Connection.Database.Password),
    cfg.Streaming.SlotName, cfg.Streaming.Publication, cfg.Streaming.ClampChannelCapacity());

// ---------- Наблюдение ----------

app.MapGet("/api/cdc/state", (CdcStreamingEngine engine) => Results.Ok(engine.GetSnapshot()));

app.MapGet("/health", () => Results.Ok(new { status = "Healthy" }));

// ready = процесс жив, БД отвечает и слот существует/валиден; при терминальной
// ошибке (error_slot/failed) — 503 с диагностикой
app.MapGet("/ready", async (ReplicationGateway gateway, IOptions<CdcStreamingOptions> options) =>
{
    try
    {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var status = await gateway.SlotStatusAsync(timeoutCts.Token);
        if (status is null)
            return Results.Problem(title: "Not Ready", detail: "cdc.outbox_slot_status() не вернул данные", statusCode: 503);
        if (!status.Exists)
            return Results.Problem(title: "Not Ready",
                detail: $"слот {options.Value.Streaming.SlotName} не существует", statusCode: 503);
        if (status.IsInvalidated)
            return Results.Problem(title: "Not Ready",
                detail: $"слот {options.Value.Streaming.SlotName} инвалидирован (wal_status={status.WalStatus}); WAL утрачен",
                statusCode: 503);
        return Results.Ok(new { status = "Ready", walStatus = status.WalStatus });
    }
    catch (Exception ex)
    {
        return Results.Problem(title: "Not Ready", detail: ex.Message, statusCode: 503);
    }
});

app.MapPrometheusScrapingEndpoint(); // /metrics — pull для Prometheus

await app.RunAsync();
