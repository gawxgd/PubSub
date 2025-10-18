using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using SchemaRegistry.Domain.Models;
using SchemaRegistry.Infrastructure.Storage;
using SchemaRegistry.Infrastructure.Validation;
using System.Collections.Generic;

namespace SchemaRegistry.Domain.Services.Implementations
{
    public class SchemaRegistryService : ISchemaRegistryService
    {
        private readonly ISchemaStore _store;
        private readonly ICompatibilityChecker _checker;
        private readonly string _compatMode; // "BACKWARD", "FORWARD", "FULL", "NONE"

        public SchemaRegistryService(ISchemaStore store, ICompatibilityChecker checker, Microsoft.Extensions.Configuration.IConfiguration cfg)
        {
            _store = store;
            _checker = checker;
            _compatMode = cfg.GetValue<string>("SchemaRegistry:CompatibilityMode") ?? "BACKWARD";
        }

        public async Task<int> RegisterSchemaAsync(string topic, string schemaJson)
        {
            if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("topic");
            if (string.IsNullOrWhiteSpace(schemaJson)) throw new ArgumentException("schemaJson");

            var checksum = ComputeChecksum(schemaJson);

            // If same schema exists globally -> return its id (dedupe)
            var existing = await _store.GetByChecksumAsync(checksum);
            if (existing != null)
                return existing.Id;

            // get latest for topic to check compatibility
            var latest = await _store.GetLatestForTopicAsync(topic);

            if (latest != null && !IsCompatible(latest.SchemaJson, schemaJson))
            {
                throw new SchemaCompatibilityException("New schema is not compatible with latest for topic.");
            }

            // TODO: maybe check if the schema json is valid if we are to create a new one?
            
            // create new id (store will assign)
            var entity = new SchemaEntity
            {
                Topic = topic,
                SchemaJson = schemaJson,
                Checksum = checksum,
                CreatedAt = DateTime.UtcNow
            };

            var created = await _store.SaveAsync(entity);
            return created.Id;
        }

        public Task<SchemaEntity?> GetLatestSchemaAsync(string subject) =>
            _store.GetLatestForTopicAsync(subject);

        public Task<SchemaEntity?> GetSchemaByIdAsync(int id) =>
            _store.GetByIdAsync(id);

        public Task<IEnumerable<SchemaEntity>> GetVersionsAsync(string subject) =>
            _store.GetAllForTopicAsync(subject);

        private static string ComputeChecksum(string text)
        {
            using var sha = SHA256.Create();
            var bytes = Encoding.UTF8.GetBytes(text);
            var hash = sha.ComputeHash(bytes);
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }

        private bool IsCompatible(string writerSchemaJson, string newWriterSchemaJson)
        {
            // Interpret compatibility based on global _compatMode
            if (_compatMode.Equals("NONE", StringComparison.OrdinalIgnoreCase)) return true;

            // Depending on requested mode, call checker with writer/reader in proper order:
            // For BACKWARD: newWriter (writer) must be readable by old readers -> check new vs old (writer=new, reader=old)
            if (_compatMode.Equals("BACKWARD", StringComparison.OrdinalIgnoreCase))
            {
                return _checker.IsBackwardCompatible(newWriterSchemaJson, writerSchemaJson);
            }
            if (_compatMode.Equals("FORWARD", StringComparison.OrdinalIgnoreCase))
            {
                return _checker.IsForwardCompatible(newWriterSchemaJson, writerSchemaJson);
            }
            if (_compatMode.Equals("FULL", StringComparison.OrdinalIgnoreCase))
            {
                return _checker.IsBackwardCompatible(newWriterSchemaJson, writerSchemaJson)
                    && _checker.IsForwardCompatible(newWriterSchemaJson, writerSchemaJson);
            }
            // default conservative
            return false;
        }
    }

    public class SchemaCompatibilityException : Exception
    {
        public SchemaCompatibilityException(string message) : base(message) { }
    }
}
