Feature: Schema Full compatibility upgrade paths
    These scenarios combine Schema Registry enforcement (FULL) with end-to-end publish/subscribe flow.
    We keep them split into two directions

    @schemaRegistryMode_FULL
    Scenario: FULL A→B - old subscriber reads messages after schema adds a field with default
        Given a subscriber of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        Given publisher oldPub of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher oldPub sends message "seed" to topic "test-topic"
        Then the subscriber successfully receives 1 messages
        Given publisher newPub of type "TestEventWithAdditionalDefaultField" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher newPub sends message "message-content" priority 0 to topic "test-topic"
        Then the subscriber successfully receives 1 messages

    @schemaRegistryMode_FULL
    Scenario: FULL B→A - old subscriber reads messages after schema removes a defaulted field
        Given a subscriber of type "TestEventWithAdditionalDefaultField" is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        Given publisher oldPub of type "TestEventWithAdditionalDefaultField" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher oldPub sends message "seed" priority 0 to topic "test-topic"
        Then the subscriber successfully receives 1 messages
        Given publisher newPub of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher newPub sends message "message-content" to topic "test-topic"
        Then the subscriber receives message "message-content" with default priority

