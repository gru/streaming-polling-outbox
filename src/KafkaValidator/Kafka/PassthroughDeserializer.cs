using KafkaFlow;

namespace KafkaValidator.Kafka;

/// <summary>
/// Десериализатор-сквозняк (зеркало PassthroughSerializer у CDC-сервисов):
/// value приходит готовыми байтами конверта (Envelope.Serialize) — отдаём их
/// как byte[], разбор выполняет ValidationHandler.
/// </summary>
public sealed class PassthroughDeserializer : IDeserializer
{
    public async Task<object> DeserializeAsync(Stream input, Type type, ISerializerContext context)
    {
        using var buffer = new MemoryStream();
        await input.CopyToAsync(buffer);
        return buffer.ToArray();
    }
}
