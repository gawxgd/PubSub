using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using SchemaRegistry.Domain.Exceptions;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Inbound.DTOs;
using SchemaRegistry.Outbound.DTOs;

namespace SchemaRegistry.Controllers;

[ApiController]
[Route("topics/{topic}/versions")] // TODO: rethink routes
public class SchemasController(ISchemaRegistryService schemaRegistryService) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Register(string topic, [FromBody] RegisterRequest req)
    {
        try
        {
            var id = await schemaRegistryService.RegisterSchemaAsync(topic, req.Schema);
            return Ok(new { id });
        }
        catch (SchemaCompatibilityException ex)
        {
            return Conflict(new { error = ex.Message });
        }
    }

    [HttpGet("latest")]
    public async Task<IActionResult> GetLatest(string topic)
    {
        var s = await schemaRegistryService.GetLatestSchemaAsync(topic);

        if (s == null)
            return NotFound();

        var dto = new SchemaDto
        {
            Id = s.Id,
            Version = s.Version,
            SchemaJson = JsonDocument.Parse(s.SchemaJson).RootElement
        };

        return Ok(dto);
    }

    [HttpGet("~/schemas/ids/{id}")]
    public async Task<IActionResult> GetById(int id)
    {
        var s = await schemaRegistryService.GetSchemaByIdAsync(id);

        if (s == null)
            return NotFound();

        var dto = new SchemaDto
        {
            Id = s.Id,
            Version = s.Version,
            Topic = s.Topic,
            SchemaJson = JsonDocument.Parse(s.SchemaJson).RootElement
        };

        return Ok(dto);
    }
}