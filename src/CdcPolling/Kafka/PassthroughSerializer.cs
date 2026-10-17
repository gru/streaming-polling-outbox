using System.Text;
using KafkaFlow;

namespace CdcPolling.Kafka;

/// <summary>
/// Сериализатор-сквозняк: value приходит уже готовыми байтами конверта
/// (Envelope.Serialize), key — строка id. Сериализация в байты выполняется здесь,
/// чтобы KafkaFlow отдал их в Kafka без изменений (JsonCore-сериализатор
/// экранировал бы JSON строкой).
/// </summary>
public sealed class PassthroughSerializer : ISerializer
{
    public Task SerializeAsync(object message, Stream output, ISerializerContext context)
    {
        switch (message)
        {
            case byte[] bytes:
                output.Write(bytes, 0, bytes.Length);
                break;
            case string s:
                var b = Encoding.UTF8.GetBytes(s);
                output.Write(b, 0, b.Length);
                break;
            case null:
                break; // tombstone не используется, но не падаем
            default:
                throw new InvalidOperationException(
                    $"PassthroughSerializer ожидает byte[]/string, получил {message.GetType().FullName}");
        }
        return Task.CompletedTask;
    }
}
