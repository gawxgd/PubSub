using Microsoft.AspNetCore.Mvc;
using SchemaRegistry.Domain.Services;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using SchemaRegistry.Domain.Services.Implementations;

namespace SchemaRegistry.Controllers
{
    [ApiController]
    [Route("topics/{topic}/versions")] // TODO: rethink routes
    public class SchemasController : ControllerBase
    {
        private readonly ISchemaRegistryService _schemaRegistryService;
        public SchemasController(ISchemaRegistryService schemaRegistryService) => _schemaRegistryService = schemaRegistryService;

        public class RegisterRequest
        {
            [JsonPropertyName("schema")] // TODO: is this thing worth creating a class? 
            public string Schema { get; set; } = null!;
        }

        [HttpPost]
        public async Task<IActionResult> Register(string topic, [FromBody] RegisterRequest req)
        {
            try
            {
                var id = await _schemaRegistryService.RegisterSchemaAsync(topic, req.Schema);
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
            var s = await _schemaRegistryService.GetLatestSchemaAsync(topic);
            if (s == null) return NotFound();
            return Ok(new { id = s.Id, version = s.Version, schema = System.Text.Json.JsonDocument.Parse(s.SchemaJson).RootElement }); //TODO: dto?
        }
        
        [HttpGet("~/schemas/ids/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var s = await _schemaRegistryService.GetSchemaByIdAsync(id);
            if (s == null) return NotFound();
            return Ok(new { id = s.Id, version = s.Version, subject = s.Topic, schema = System.Text.Json.JsonDocument.Parse(s.SchemaJson).RootElement });
        }
    }
}
