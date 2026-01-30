using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace PerformanceTests.Infrastructure;

public class KafkaJsonSerializer<T> : ISerializer<T>
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false
    };

    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
            return null!;

        try
        {
            var json = JsonSerializer.Serialize(data, Options);
            return Encoding.UTF8.GetBytes(json);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to serialize {typeof(T).Name} to JSON: {ex.Message}", ex);
        }
    }
}

public class KafkaJsonDeserializer<T> : IDeserializer<T>
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = null,
        PropertyNameCaseInsensitive = true
    };

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty)
            return default!;

        try
        {
            var json = Encoding.UTF8.GetString(data);
            var result = JsonSerializer.Deserialize<T>(json, Options);
            
            if (result == null)
            {
                throw new InvalidOperationException($"Deserialization returned null for JSON: {json.Substring(0, Math.Min(100, json.Length))}...");
            }
            
            return result;
        }
        catch (JsonException ex)
        {
            var jsonPreview = Encoding.UTF8.GetString(data.Slice(0, Math.Min(200, data.Length)));
            throw new InvalidOperationException($"Failed to deserialize JSON to {typeof(T).Name}. JSON preview: {jsonPreview}... Error: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to deserialize to {typeof(T).Name}: {ex.Message}", ex);
        }
    }
}

