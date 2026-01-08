Feature: Subscriber Offset Positions
    As a system integrator
    I want to verify that multiple subscribers maintain their own positions independently
    So that each subscriber can consume messages at its own pace without affecting others

    Scenario: Subscribers maintain their own positions
        Given a publisher is configured with the following options:
            | Setting            | Value          |
            | Topic              | default        |
            | Broker             | 127.0.0.1:9096 |
            | Queue Size         | 1000           |
            | Max Retry Attempts | 3              |
            | Max Send Attempts  | 3              |
        And 11 messages have been published to topic "default"
        And subscriber S1 is configured starting at offset 11 with the following options:
            | Setting            | Value          |
            | Topic              | default        |
            | Broker             | 127.0.0.1:9098 |
            | Poll Interval      | 100            |
            | Max Retry Attempts | 3              |
        And subscriber S2 is configured starting at offset 5 with the following options:
            | Setting            | Value          |
            | Topic              | default        |
            | Broker             | 127.0.0.1:9098 |
            | Poll Interval      | 100            |
            | Max Retry Attempts | 3              |
        When the publisher sends message "m11" to topic "default"
        Then subscriber S1 should receive only message "m11"
        And subscriber S2 should receive messages "msg5" to "m11":
            | Message |
            | msg5    |
            | msg6    |
            | msg7    |
            | msg8    |
            | msg9    |
            | msg10   |
            | m11     |

