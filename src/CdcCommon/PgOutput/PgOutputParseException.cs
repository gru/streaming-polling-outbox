namespace CdcCommon.PgOutput;

/// <summary>
/// Ошибка разбора бинарной записи pgoutput: структура не соответствует
/// протоколу proto_version=1 либо данные outbox-строки не проходят валидацию.
/// Ошибка фатальна для сервиса (не транзиентна): повторный peek вернёт те же байты.
/// </summary>
public sealed class PgOutputParseException(string message, Exception? inner = null)
    : Exception(message, inner);
