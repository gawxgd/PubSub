Feature: Publisher and Subscriber Communication
    As a system integrator
    I want to verify that publishers can send messages and subscribers can receive them
    So that the message broker correctly routes messages between components

    Scenario: Publisher sends message and subscriber receives it with default configuration
        Given a publisher is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Min Message Length  | 1              |
            | Max Message Length  | 1024           |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        When the publisher sends message "Hello World" to topic "test-topic"
        Then a subscriber receives message "Hello World" from topic "test-topic"

    Scenario: Ordered delivery per partition
        Given a publisher is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Min Message Length  | 1              |
            | Max Message Length  | 1024           |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        When the publisher sends messages in order:
            | Message |
            | p1      |
            | p2      |
            | p3      |
        Then the subscriber receives messages in order:
            | Message |
            | p1      |
            | p2      |
            | p3      |