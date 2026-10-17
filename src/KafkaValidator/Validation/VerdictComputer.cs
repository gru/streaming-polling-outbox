namespace KafkaValidator.Validation;

/// <summary>
/// Чистая функция вердикта: фактические записи прогона против ожидаемого набора.
/// Не знает ни Kafka, ни HTTP — полностью покрывается unit-тестами.
/// </summary>
public static class VerdictComputer
{
    public const int SampleLimit = 20;

    public static ValidationVerdict Compute(Guid runId, RunDetail? tallies, ExpectedSet expected, DateTimeOffset computedAtUtc)
    {
        var kafkaIds = tallies?.UniqueIds ?? new HashSet<Guid>();

        // Прогон не замечен в Kafka вовсе → пустое множество: все written станут
        // доказанной потерей (в т.ч. случай «в Kafka вообще ничего из прогона»)
        var missing = expected.WrittenIds.Except(kafkaIds).OrderBy(x => x).ToList();
        var ambiguousDelivered = expected.AmbiguousIds.Intersect(kafkaIds).Count();
        var ambiguousNotDelivered = expected.AmbiguousIds.Except(kafkaIds).Count();

        // known = written ∪ ambiguous. failed НЕ включаем: доказанный Writer'ом
        // незафиксированный commit не мог попасть в WAL/Kafka — запись с таким id
        // невозможна при корректном CDC и обязана попасть в unexpectedExtra
        var knownIds = expected.WrittenIds
            .Concat(expected.AmbiguousIds)
            .ToHashSet();
        var unexpectedExtra = kafkaIds.Except(knownIds).OrderBy(x => x).ToList();

        var pass = missing.Count == 0 && unexpectedExtra.Count == 0;

        return new ValidationVerdict
        {
            RunId = runId,
            Verdict = pass ? "pass" : "fail",
            ComputedAtUtc = computedAtUtc,
            Kafka = new ValidationVerdict.KafkaCounts
            {
                Records = tallies?.Records ?? 0,
                UniqueIds = kafkaIds.Count,
                DuplicateRecords = (tallies?.Records ?? 0) - kafkaIds.Count,
            },
            Expected = new ValidationVerdict.ExpectedCounts
            {
                Source = expected.Source,
                WrittenCount = expected.WrittenIds.Count,
                AmbiguousCount = expected.AmbiguousIds.Count,
                FailedCount = expected.FailedIds.Count,
            },
            MissingWrittenIdsCount = missing.Count,
            MissingWrittenIdsSample = missing.Take(SampleLimit).ToList(),
            AmbiguousDeliveredCount = ambiguousDelivered,
            AmbiguousNotDeliveredCount = ambiguousNotDelivered,
            UnexpectedExtraIdsCount = unexpectedExtra.Count,
            UnexpectedExtraIdsSample = unexpectedExtra.Take(SampleLimit).ToList(),
        };
    }
}
