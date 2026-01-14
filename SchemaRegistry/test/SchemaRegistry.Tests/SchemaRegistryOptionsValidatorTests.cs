using System.Collections.Generic;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using SchemaRegistry.Domain.Enums;
using Xunit;

namespace SchemaRegistry.Tests;

public sealed class SchemaRegistryOptionsValidatorTests
{
    [Fact]
    public void Validate_ShouldSucceed_WhenCompatibilityModeMissing()
    {
        var cfg = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        var validator = new SchemaRegistryOptionsValidator(cfg, NullLogger<SchemaRegistryOptionsValidator>.Instance);
        var result = validator.Validate(name: null, new SchemaRegistryOptions { CompatibilityMode = CompatibilityMode.Full });

        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_ShouldFail_WhenCompatibilityModeInvalid()
    {
        var cfg = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SchemaRegistry:CompatibilityMode"] = "NOT_A_MODE"
            })
            .Build();

        var validator = new SchemaRegistryOptionsValidator(cfg, NullLogger<SchemaRegistryOptionsValidator>.Instance);
        var result = validator.Validate(name: null, new SchemaRegistryOptions { CompatibilityMode = CompatibilityMode.Full });

        result.Failed.Should().BeTrue();
    }
}

