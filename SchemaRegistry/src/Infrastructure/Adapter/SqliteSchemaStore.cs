using Microsoft.Data.Sqlite;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Domain.Port;

namespace SchemaRegistry.Infrastructure.Adapter;

public sealed class SqliteSchemaStore : ISchemaStore
{
    private readonly string _connectionString;

    public SqliteSchemaStore(string connectionString)
    {
        _connectionString = connectionString;
        InitializeDatabase();
    }

    private void InitializeDatabase()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS schemas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                version INTEGER NOT NULL,
                checksum TEXT NOT NULL UNIQUE,
                schema_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_schemas_topic_version
                ON schemas(topic, version);

            CREATE INDEX IF NOT EXISTS ix_schemas_topic
                ON schemas(topic);
        """;

        command.ExecuteNonQuery();
    }
    
    private SchemaEntity MapDbRecordToSchema(SqliteDataReader reader)
        => new()
        {
            Id = reader.GetInt32(0),
            Topic = reader.GetString(1),
            Version = reader.GetInt32(2),
            Checksum = reader.GetString(3),
            SchemaJson = reader.GetString(4),
            CreatedAt = DateTime.Parse(reader.GetString(5))
        };

    public async Task<SchemaEntity?> GetByIdAsync(int id)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, topic, version, checksum, schema_json, created_at
            FROM schemas
            WHERE id = @id
        """;
        cmd.Parameters.AddWithValue("@id", id);

        using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? MapDbRecordToSchema(reader) : null;
    }

    public async Task<SchemaEntity?> GetByChecksumAsync(string checksum)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, topic, version, checksum, schema_json, created_at
            FROM schemas
            WHERE checksum = @checksum
        """;
        cmd.Parameters.AddWithValue("@checksum", checksum);

        using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? MapDbRecordToSchema(reader) : null;
    }

    public async Task<SchemaEntity?> GetLatestForTopicAsync(string topic)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, topic, version, checksum, schema_json, created_at
            FROM schemas
            WHERE topic = @topic
            ORDER BY version DESC
            LIMIT 1
        """;
        cmd.Parameters.AddWithValue("@topic", topic);

        using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? MapDbRecordToSchema(reader) : null;
    }

    public async Task<IEnumerable<SchemaEntity>> GetAllForTopicAsync(string topic)
    {
        var result = new List<SchemaEntity>();

        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, topic, version, checksum, schema_json, created_at
            FROM schemas
            WHERE topic = @topic
            ORDER BY version DESC
        """;
        cmd.Parameters.AddWithValue("@topic", topic);

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(MapDbRecordToSchema(reader));
        }

        return result;
    }

    public async Task<SchemaEntity> SaveAsync(SchemaEntity entity)
    {
        using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        using var transaction = connection.BeginTransaction();

        // next version for topic
        var versionCmd = connection.CreateCommand();
        versionCmd.Transaction = transaction;
        versionCmd.CommandText = """
            SELECT COALESCE(MAX(version), 0)
            FROM schemas
            WHERE topic = @topic
        """;
        versionCmd.Parameters.AddWithValue("@topic", entity.Topic);

        var nextVersion = Convert.ToInt32(await versionCmd.ExecuteScalarAsync()) + 1;

        var insertCmd = connection.CreateCommand();
        insertCmd.Transaction = transaction;
        insertCmd.CommandText = """
            INSERT INTO schemas (topic, version, checksum, schema_json, created_at)
            VALUES (@topic, @version, @checksum, @schemaJson, @createdAt);
            SELECT last_insert_rowid();
        """;

        insertCmd.Parameters.AddWithValue("@topic", entity.Topic);
        insertCmd.Parameters.AddWithValue("@version", nextVersion);
        insertCmd.Parameters.AddWithValue("@checksum", entity.Checksum);
        insertCmd.Parameters.AddWithValue("@schemaJson", entity.SchemaJson);
        insertCmd.Parameters.AddWithValue("@createdAt", DateTime.UtcNow.ToString("O"));

        var id = Convert.ToInt32(await insertCmd.ExecuteScalarAsync());

        await transaction.CommitAsync();

        entity.Id = id;
        entity.Version = nextVersion;
        entity.CreatedAt = DateTime.UtcNow;

        return entity;
    }
}
