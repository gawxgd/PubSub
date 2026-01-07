Feature: Schema backward compatibility
    # Reading messages serialized with an older schema 
    
    Scenario:  Backward compatibility - Subscriber with v2 schema reads messages from both v1 and v2 publishers
        Given a publisher is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
    When schema v1 is registered for topic "test-topic"
        When the publisher sends 5 messages to topic "test-topic"
        And schema v2 is registered for topic "test-topic"
        And the publisher restarts
        And the publisher sends 5 messages to topic "test-topic"
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        Then the subscriber successfully receives 10 messages