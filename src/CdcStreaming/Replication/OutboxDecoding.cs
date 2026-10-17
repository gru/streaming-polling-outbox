using CdcCommon;
using CdcCommon.PgOutput;

namespace CdcStreaming.Replication;

/// <summary>
/// Карта колонок public.outbox_messages по именам (чистое ядро декодера,
/// без типов Npgsql — покрывается unit-тестами). pgoutput присылает значения
/// позиционно в порядке колонок Relation-сообщения, поэтому маппинг — это
/// индексы четырёх обязательных колонок внутри этого порядка.
/// </summary>
public sealed class OutboxColumnMap
{
    public int IdIndex { get; }
    public int TopicIndex { get; }
    public int BodyIndex { get; }
    public int CreatedAtIndex { get; }

    private OutboxColumnMap(int id, int topic, int body, int createdAt)
    {
        IdIndex = id;
        TopicIndex = topic;
        BodyIndex = body;
        CreatedAtIndex = createdAt;
    }

    /// <summary>
    /// Все четыре колонки обязательны (таблица 01-db-publication.sql: NOT NULL);
    /// отсутствие любой из них — фатальная ошибка декодирования, повторный
    /// стрим вернёт те же сообщения.
    /// </summary>
    public static OutboxColumnMap Create(IReadOnlyList<string> columnNames)
    {
        var id = IndexOf(columnNames, "id");
        var topic = IndexOf(columnNames, "topic");
        var body = IndexOf(columnNames, "body");
        var createdAt = IndexOf(columnNames, "created_at");
        if (id < 0 || topic < 0 || body < 0 || createdAt < 0)
            throw new PgOutputParseException(
                $"Relation public.outbox_messages без обязательных колонок (id={id >= 0}, topic={topic >= 0}, " +
                $"body={body >= 0}, created_at={createdAt >= 0}); колонки: [{string.Join(", ", columnNames)}]");
        return new OutboxColumnMap(id, topic, body, createdAt);
    }

    /// <summary>Колонка на позиции position в Insert (false — позиция вне карты: лишние значения пропускаем).</summary>
    public bool TryGetColumn(int position, out OutboxColumn column)
    {
        if (position == IdIndex) { column = OutboxColumn.Id; return true; }
        if (position == TopicIndex) { column = OutboxColumn.Topic; return true; }
        if (position == BodyIndex) { column = OutboxColumn.Body; return true; }
        if (position == CreatedAtIndex) { column = OutboxColumn.CreatedAt; return true; }
        column = default;
        return false;
    }

    private static int IndexOf(IReadOnlyList<string> names, string column)
    {
        for (var i = 0; i < names.Count; i++)
            if (string.Equals(names[i], column, StringComparison.Ordinal))
                return i;
        return -1;
    }
}

public enum OutboxColumn
{
    Id,
    Topic,
    Body,
    CreatedAt,
}

/// <summary>
/// Сборка OutboxEvent из значений Insert-строки (чистое ядро). Все значения
/// приходят текстом: pgoutput proto v1 передаёт колонки в текстовом формате,
/// и Npgsql размечает их как text (типизированные Get&lt;Guid&gt;/Get&lt;DateTime&gt;
/// не поддерживаются) — читаем Get&lt;string&gt; и парсим сами, timestamptz тем же
/// PgOutputParser.ParseTimestamp, что и Polling. Значение null — колонка пришла
/// видом NULL/unchanged-TOAST: для outbox-строки это фатально (все колонки
/// NOT NULL — та же семантика, что у парсера Polling).
/// </summary>
public static class OutboxRowFactory
{
    /// <summary>Единственный разрешённый topic (CHECK в таблице; вторая линия защиты от произвольной маршрутизации).</summary>
    public const string AllowedTopic = "outbox.events";

    public static OutboxEvent Create(
        string? idText, string? topic, string? body, string? createdAtText, uint xid, Lsn commitLsn)
    {
        if (idText is null)
            throw new PgOutputParseException($"колонка id пуста (NULL или unchanged-TOAST) в Insert outbox-строки (xid={xid})");
        if (topic is null)
            throw new PgOutputParseException($"колонка topic пуста (NULL или unchanged-TOAST) в Insert outbox-строки (xid={xid})");
        if (body is null)
            throw new PgOutputParseException($"колонка body пуста (NULL или unchanged-TOAST) в Insert outbox-строки (xid={xid})");
        if (createdAtText is null)
            throw new PgOutputParseException($"колонка created_at пуста (NULL или unchanged-TOAST) в Insert outbox-строки (xid={xid})");
        if (!Guid.TryParse(idText, out var id))
            throw new PgOutputParseException($"колонка id не является UUID: '{idText}' (xid={xid})");
        if (!string.Equals(topic, AllowedTopic, StringComparison.Ordinal))
            throw new PgOutputParseException(
                $"запрещённый topic '{topic}' в outbox-строке (допустим только '{AllowedTopic}'; xid={xid})");

        var createdAt = PgOutputParser.ParseTimestamp(createdAtText); // PgOutputParseException при мусоре
        return new OutboxEvent(id, topic, body, createdAt, commitLsn, xid);
    }
}
