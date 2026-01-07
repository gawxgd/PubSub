Feature: Multiple Publisher and Subscriber Communication
    As a system integrator
    I want to verify that multiple publishers can send messages and multiple subscribers can receive them
    So that the message broker correctly routes messages between multiple components

    Scenario: Multiple Publisher Subscriber
        Given publishers A, B, C are configured with the following options:
            | Setting             | Value          |
            | Topic               | default        |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And subscribers D, E, F are configured with the following options:
            | Setting             | Value          |
            | Topic               | default        |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        When the publisher A sends messages in order:
            | Message |
            | p1      |
            | p2      |
            | p3      |
        When the publisher B sends messages in order:
            | Message |
            | p4      |
            | p5      |
            | p6      |
        When the publisher C sends messages in order:
            | Message |
            | p7      |
            | p8      |
            | p9      |
        Then the subscriber D receives messages:
            | Message |
            | p1      |
            | p2      |
            | p3      |
            | p4      |
            | p5      |
            | p6      |
            | p7      |
            | p8      |
            | p9      |
        Then the subscriber E receives messages:
            | Message |
            | p1      |
            | p2      |
            | p3      |
            | p4      |
            | p5      |
            | p6      |
            | p7      |
            | p8      |
            | p9      |
        Then the subscriber F receives messages:
            | Message |
            | p1      |
            | p2      |
            | p3      |
            | p4      |
            | p5      |
            | p6      |
            | p7      |
            | p8      |
            | p9      |

