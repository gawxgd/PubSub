Feature: Multiple Topics Publisher and Subscriber Communication
    As a system integrator
    I want to verify that multiple publishers and subscribers can communicate across different topics simultaneously
    So that the message broker correctly isolates and routes messages per topic without cross-contamination

    Scenario: Multiple publishers and subscribers across multiple topics simultaneously
        Given publishers A1, B1, C1 are configured with the following options:
            | Setting            | Value          |
            | Topic              | default        |
            | Broker             | 127.0.0.1:9096 |
            | Queue Size         | 1000           |
            | Max Retry Attempts | 3              |
            | Max Send Attempts  | 3              |
        And subscribers D1, E1, F1 are configured with the following options:
            | Setting            | Value          |
            | Topic              | default        |
            | Broker             | 127.0.0.1:9098 |
            | Poll Interval      | 100            |
            | Max Retry Attempts | 3              |
        And publishers A2, B2, C2 are configured with the following options:
            | Setting            | Value          |
            | Topic              | test-topic     |
            | Broker             | 127.0.0.1:9096 |
            | Queue Size         | 1000           |
            | Max Retry Attempts | 3              |
            | Max Send Attempts  | 3              |
        And subscribers D2, E2, F2 are configured with the following options:
            | Setting            | Value          |
            | Topic              | test-topic     |
            | Broker             | 127.0.0.1:9098 |
            | Poll Interval      | 100            |
            | Max Retry Attempts | 3              |
        And publishers A3, B3, C3 are configured with the following options:
            | Setting            | Value          |
            | Topic              | custom-topic   |
            | Broker             | 127.0.0.1:9096 |
            | Queue Size         | 1000           |
            | Max Retry Attempts | 3              |
            | Max Send Attempts  | 3              |
        And subscribers D3, E3, F3 are configured with the following options:
            | Setting            | Value          |
            | Topic              | custom-topic   |
            | Broker             | 127.0.0.1:9098 |
            | Poll Interval      | 100            |
            | Max Retry Attempts | 3              |
        When the publisher A1 sends messages in order:
            | Message |
            | d1      |
            | d2      |
            | d3      |
        And the publisher B1 sends messages in order:
            | Message |
            | d4      |
            | d5      |
            | d6      |
        And the publisher C1 sends messages in order:
            | Message |
            | d7      |
            | d8      |
            | d9      |
        And the publisher A2 sends messages in order:
            | Message |
            | t1      |
            | t2      |
            | t3      |
        And the publisher B2 sends messages in order:
            | Message |
            | t4      |
            | t5      |
            | t6      |
        And the publisher C2 sends messages in order:
            | Message |
            | t7      |
            | t8      |
            | t9      |
        And the publisher A3 sends messages in order:
            | Message |
            | c1      |
            | c2      |
            | c3      |
        And the publisher B3 sends messages in order:
            | Message |
            | c4      |
            | c5      |
            | c6      |
        And the publisher C3 sends messages in order:
            | Message |
            | c7      |
            | c8      |
            | c9      |
        Then the subscriber D1 receives messages:
            | Message |
            | d1      |
            | d2      |
            | d3      |
            | d4      |
            | d5      |
            | d6      |
            | d7      |
            | d8      |
            | d9      |
        And the subscriber E1 receives messages:
            | Message |
            | d1      |
            | d2      |
            | d3      |
            | d4      |
            | d5      |
            | d6      |
            | d7      |
            | d8      |
            | d9      |
        And the subscriber F1 receives messages:
            | Message |
            | d1      |
            | d2      |
            | d3      |
            | d4      |
            | d5      |
            | d6      |
            | d7      |
            | d8      |
            | d9      |
        And the subscriber D2 receives messages:
            | Message |
            | t1      |
            | t2      |
            | t3      |
            | t4      |
            | t5      |
            | t6      |
            | t7      |
            | t8      |
            | t9      |
        And the subscriber E2 receives messages:
            | Message |
            | t1      |
            | t2      |
            | t3      |
            | t4      |
            | t5      |
            | t6      |
            | t7      |
            | t8      |
            | t9      |
        And the subscriber F2 receives messages:
            | Message |
            | t1      |
            | t2      |
            | t3      |
            | t4      |
            | t5      |
            | t6      |
            | t7      |
            | t8      |
            | t9      |
        And the subscriber D3 receives messages:
            | Message |
            | c1      |
            | c2      |
            | c3      |
            | c4      |
            | c5      |
            | c6      |
            | c7      |
            | c8      |
            | c9      |
        And the subscriber E3 receives messages:
            | Message |
            | c1      |
            | c2      |
            | c3      |
            | c4      |
            | c5      |
            | c6      |
            | c7      |
            | c8      |
            | c9      |
        And the subscriber F3 receives messages:
            | Message |
            | c1      |
            | c2      |
            | c3      |
            | c4      |
            | c5      |
            | c6      |
            | c7      |
            | c8      |
            | c9      |

