using System.Globalization;

namespace CdcCommon;

/// <summary>
/// LSN (Log Sequence Number) PostgreSQL: текстовый формат "X/YYYYYYYY" (обе части — hex).
/// Используется для позиций слота (confirmed_flush_lsn, restart_lsn), advance и метрик WAL lag.
/// </summary>
public readonly record struct Lsn(ulong Value) : IComparable<Lsn>
{
    public static readonly Lsn Zero = new(0);

    public static Lsn Parse(string text) =>
        TryParse(text) ?? throw new FormatException($"некорректный pg_lsn: '{text}'");

    public static Lsn? TryParse(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return null;
        var slash = text.IndexOf('/');
        if (slash <= 0 || slash == text.Length - 1)
            return null;
        if (!ulong.TryParse(text.AsSpan(0, slash), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var hi))
            return null;
        if (!ulong.TryParse(text.AsSpan(slash + 1), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var lo))
            return null;
        if (lo > 0xFFFFFFFFUL) // младшая часть — 8 hex-цифр максимум
            return null;
        return new Lsn((hi << 32) | lo);
    }

    public int CompareTo(Lsn other) => Value.CompareTo(other.Value);

    public static bool operator <(Lsn left, Lsn right) => left.Value < right.Value;
    public static bool operator >(Lsn left, Lsn right) => left.Value > right.Value;
    public static bool operator <=(Lsn left, Lsn right) => left.Value <= right.Value;
    public static bool operator >=(Lsn left, Lsn right) => left.Value >= right.Value;

    public override string ToString() =>
        // формат вывода pg_lsn (print_pg_lsn): старшая и младшая части без дополнения нулями
        $"{(Value >> 32).ToString("X", CultureInfo.InvariantCulture)}/{(Value & 0xFFFFFFFFUL).ToString("X", CultureInfo.InvariantCulture)}";
}
