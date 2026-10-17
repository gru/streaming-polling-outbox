namespace KafkaValidator.Validation;

/// <summary>
/// Вердикт валидации прогона — фактическое содержимое Kafka против ожидаемого
/// набора id (requirements.md 15). Минимум §15: общее количество записей,
/// уникальные id, дубликаты, отсутствующие id, записанные Writer, подтверждённые
/// CDC (снапшот CDC — из HTTP API сервиса, не из Prometheus).
///
/// Семантика потерь (§15: «отделяй неопределённые результаты записи от
/// доказанных потерь CDC»):
///   MissingWrittenIds      — Writer ДОКАЗАЛ commit (written), записи в Kafka
///                            нет → доказанная потеря CDC;
///   Ambiguous*             — исход commit у Writer не определён: и доставленные
///                            (commit всё-таки случился), и недоставленные
///                            (скорее всего commit не случился) НЕ являются
///                            доказанной потерей CDC;
///   UnexpectedExtra        — id в Kafka вне всех списков Writer — аномалия
///                            (доказанная лишняя доставка);
///   DuplicateRecords       — записи сверх уникальных id: допустимо (at-least-once,
///                            §10), на вердикт не влияет.
/// Вердикт pass ⇔ нет доказанных потерь и нет неожидаемых лишних id.
/// </summary>
public sealed class ValidationVerdict
{
    public Guid RunId { get; set; }

    /// <summary>pass | fail.</summary>
    public string Verdict { get; set; } = "";

    public DateTimeOffset ComputedAtUtc { get; set; }

    public KafkaCounts Kafka { get; set; } = new();

    public ExpectedCounts Expected { get; set; } = new();

    /// <summary>Доказанные потери CDC: written в Writer, отсутствуют в Kafka (§15 «отсутствующие id»).</summary>
    public int MissingWrittenIdsCount { get; set; }

    /// <summary>Первые 20 отсутствующих id (полный список не влезает в ответ при массовой потере).</summary>
    public List<Guid> MissingWrittenIdsSample { get; set; } = new();

    /// <summary>Неопределённые коммиты Writer, которые ДОСТАВЛЕНЫ (commit всё-таки был).</summary>
    public int AmbiguousDeliveredCount { get; set; }

    /// <summary>Неопределённые коммиты Writer, которых в Kafka нет (не потеря CDC: вероятно, commit не был зафиксирован).</summary>
    public int AmbiguousNotDeliveredCount { get; set; }

    /// <summary>
    /// Записи в Kafka с id вне written/ambiguous — аномалия: Writer не отправлял
    /// такого сообщения. В частности, доставленный failed-id невозможен при
    /// корректном CDC (commit не зафиксирован → записи в WAL нет).
    /// </summary>
    public int UnexpectedExtraIdsCount { get; set; }

    public List<Guid> UnexpectedExtraIdsSample { get; set; } = new();

    /// <summary>Снапшот CDC-сервиса на момент валидации (какой arm запущен, «подтверждено CDC»); null если ни один не отвечает.</summary>
    public CdcSnapshot? Cdc { get; set; }

    public sealed class KafkaCounts
    {
        public long Records { get; set; }

        public int UniqueIds { get; set; }

        public long DuplicateRecords { get; set; }
    }

    public sealed class ExpectedCounts
    {
        /// <summary>writer | explicit.</summary>
        public string Source { get; set; } = "";

        public int WrittenCount { get; set; }

        public int AmbiguousCount { get; set; }

        public int FailedCount { get; set; }
    }

    public sealed class CdcSnapshot
    {
        /// <summary>polling | streaming.</summary>
        public string Arm { get; set; } = "";

        public string State { get; set; } = "";

        public long MessagesDelivered { get; set; }

        public string? ConfirmedLsn { get; set; }

        public long KafkaErrors { get; set; }

        public long ReadErrors { get; set; }
    }
}
