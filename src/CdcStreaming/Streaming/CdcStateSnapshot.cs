using CdcStreaming.Replication;

namespace CdcStreaming.Streaming;

/// <summary>Снимок состояния для GET /api/cdc/state: состояние, счётчики, LSN-и, слот, ошибки.</summary>
public sealed class CdcStateSnapshot
{
    public string State { get; init; } = "";
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? StateChangedAt { get; init; }
    public ConfigEcho Config { get; init; } = new();

    public long MessagesRead { get; init; }
    public long MessagesDelivered { get; init; }
    public long RecordsDecoded { get; init; }
    public long StreamSessions { get; init; }
    public long PublishBatches { get; init; }
    public long ReadErrors { get; init; }
    public long KafkaErrors { get; init; }
    public long Retries { get; init; }
    public long Reconnects { get; init; }
    public int IncompleteTransactionEvents { get; init; }
    public int ChannelDepth { get; init; }

    public string? LastReceivedLsn { get; init; }
    public string? ConfirmedLsn { get; init; }
    public long? WalLagBytes { get; init; }

    public double? LastPublishMs { get; init; }
    public double E2eP50Ms { get; init; }
    public double E2eP95Ms { get; init; }
    public double E2eP99Ms { get; init; }

    public double? LastRecoveryMs { get; init; }
    public int Recoveries { get; init; }

    public SlotStatus? Slot { get; init; }
    public string? LastError { get; init; }
    public string? LastKafkaError { get; init; }
}

public sealed class ConfigEcho
{
    public string Host { get; init; } = "";
    public int Port { get; init; }
    public string Database { get; init; } = "";
    public string Username { get; init; } = "";
    public string SlotName { get; init; } = "";
    public string Publication { get; init; } = "";
    public ulong ProtocolVersion { get; init; }
    public int ChannelCapacity { get; init; }
    public int KafkaBatchSize { get; init; }
    public string KafkaTopic { get; init; } = "";
    public string KafkaBrokers { get; init; } = "";
}
