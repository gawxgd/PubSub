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

    Scenario:  Backward compatibility 2 - Subscriber with v2 schema reads messages from both v1 and v2 publishers
        Given publishers A are configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When schema v1 is registered for topic "test-topic"
        When the publisher A sends messages in order:
            | Message |
            | p1      |
            | p2      |
            | p3      |
        And schema v2 is registered for topic "test-topic"
        And publishers B are configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher B sends messages in order:
            | Message |
            | p4      |
            | p5      |
            | p6      |
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        Then the subscriber receives messages:
            | Message |
            | p1      |
            | p2      |
            | p3      |
            | p4      |
            | p5      |
            | p6      |

    Scenario:  Backward compatibility R - Subscriber with v2 schema reads messages from both v1 and v2 publishers
        Given a publisher is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When schema v1 is registered for topic "test-topic"
        When the publisher sends messages in order:
            | Message |
            | p1      |
            | p2      |
            | p3      |
        And schema v2 is registered for topic "test-topic"
        And the publisher restarts
        When the publisher sends messages in order:
            | Message |
            | p4      |
            | p5      |
            | p6      |
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        Then the subscriber receives messages:
            | Message |
            | p1      |
            | p2      |
            | p3      |
            | p4      |
            | p5      |
            | p6      |