Feature: Commit Log Segment Management
    As a system operator
    I want the commit log to properly manage log segments
    So that large volumes of messages are handled correctly with segment rolling

    Background:
        Given a publisher is configured with the following options:
            | Setting            | Value          |
            | Topic              | default        |
            | Broker             | 127.0.0.1:9096 |
            | Queue Size         | 1000           |
            | Max Retry Attempts | 3              |
            | Max Send Attempts  | 3              |

    Scenario: Messages span multiple log segments and are persisted correctly
        When the publisher sends 15 messages with 100-byte payloads to topic "default"
        Then multiple segment files exist for topic "default"
        And commit log contains messages from offset 0 to 14

    Scenario: Subscriber reads across segment boundaries after restart
        When the publisher sends 20 messages with 100-byte payloads to topic "default"
        And the broker restarts
        Given subscriber S1 is configured starting at offset 5 with the following options:
            | Setting            | Value          |
            | Topic              | default        |
            | Broker             | 127.0.0.1:9098 |
            | Poll Interval      | 100            |
            | Max Retry Attempts | 3              |
        Then subscriber S1 receives messages from offset 5 to 19
        And reading crosses segment file boundaries seamlessly

