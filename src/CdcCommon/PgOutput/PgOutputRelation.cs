namespace CdcCommon.PgOutput;

public sealed record PgOutputColumn(int Flags, string Name, uint TypeOid, int TypeModifier);

/// <summary>
/// Relation-запись pgoutput: идентификатор relation_id и список колонок с именами.
/// Отправляется (пере)перед первым использованием relation в каждой сессии
/// декодирования — peek создаёт новую сессию на каждый вызов, поэтому кэш
/// отношений живёт в пределах одного батча.
/// </summary>
public sealed record PgOutputRelation(int RelationId, string Schema, string Name, IReadOnlyList<PgOutputColumn> Columns)
{
    public bool Is(string schema, string table) =>
        string.Equals(Schema, schema, StringComparison.Ordinal) &&
        string.Equals(Name, table, StringComparison.Ordinal);
}
