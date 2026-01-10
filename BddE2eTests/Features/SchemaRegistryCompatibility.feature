Feature: Schema Registry compatibility enforcement
    These scenarios verify that Schema Registry accepts legal schema updates
    for a given CompatibilityMode, independent of subscriber message processing.

    @schemaRegistryMode_BACKWARD
    Scenario: BACKWARD accepts removing a field (new schema registration succeeds)
        Given publisher oldSchemaPublisher of type "TestEventWithAdditionalField" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And publisher newSchemaPublisher of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher oldSchemaPublisher sends message "seed" priority 0 to topic "test-topic"
        Then publish succeeds
        When the publisher newSchemaPublisher sends message "legal-update" to topic "test-topic"
        Then publish succeeds

    @schemaRegistryMode_FORWARD
    Scenario: FORWARD accepts adding a new field (new schema registration succeeds)
        Given publisher oldSchemaPublisher of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And publisher newSchemaPublisher of type "TestEventWithAdditionalField" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher oldSchemaPublisher sends message "seed" to topic "test-topic"
        Then publish succeeds
        When the publisher newSchemaPublisher sends message "legal-update" priority 0 to topic "test-topic"
        Then publish succeeds

    @schemaRegistryMode_BACKWARD
    Scenario: BACKWARD rejects adding required field without default (new schema registration fails)
        Given publisher oldSchemaPublisher of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And publisher newSchemaPublisher of type "TestEventWithAdditionalField" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher oldSchemaPublisher sends message "seed" to topic "test-topic"
        And the publisher newSchemaPublisher sends message "should-fail" priority 0 to topic "test-topic"
        Then publish fails with schema incompatibility

    @schemaRegistryMode_FORWARD
    Scenario: FORWARD rejects removing required field without default (new schema registration fails)
        Given publisher oldSchemaPublisher of type "TestEventWithAdditionalField" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And publisher newSchemaPublisher of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher oldSchemaPublisher sends message "seed" priority 0 to topic "test-topic"
        And the publisher newSchemaPublisher sends message "should-fail" to topic "test-topic"
        Then publish fails with schema incompatibility

