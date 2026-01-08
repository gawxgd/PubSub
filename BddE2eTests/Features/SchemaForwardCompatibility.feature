Feature: Schema Forward compatibility

    Scenario: Subscriber with old schema reads message serialized with new schema with additional field
        Given schema "v2_priority_int_no_default" is registered for topic "test-topic"
        And a publisher is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher sends message "message-content" priority 0 to topic "test-topic"
        And schema "v1" is registered for topic "test-topic"
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        Then the subscriber successfully receives 1 messages

    Scenario: Subscriber with old schema reads message serialized with new schema with removed field that had a default value
        Given schema "v2_priority_int_default_0" is registered for topic "test-topic"
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        When schema "v1" is registered for topic "test-topic"
        And a publisher is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And the publisher sends message "message-content" to topic "test-topic"
        Then the subscriber successfully receives 1 messages