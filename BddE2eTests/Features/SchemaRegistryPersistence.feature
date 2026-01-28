Feature: Schema Registry persistence
    As a system integrator
    I want schema registry to persist schemas across restarts
    So that publishers/subscribers can continue to work after restarts

    Scenario: Schema Registry persists schema after restart
        Given the schema registry contains a schema for topic "test-topic"
        When schema registry restarts
        Then the schema registry still contains the same schema for topic "test-topic"

