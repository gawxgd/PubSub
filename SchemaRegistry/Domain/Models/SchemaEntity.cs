namespace SchemaRegistry.Domain.Models;

public class SchemaEntity
{
    public int Id { get; set; }

    public string Topic { get; set; }
    
    public int Version { get; set; }

    public string SchemaJson { get; set; }
    
    public string Checksum { get; set; } = null!; // TODO: how does this work?
    
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}