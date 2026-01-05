Feature: Subscriber Offset Commit and Resume
    As a system integrator
    I want to verify that subscribers can commit offsets and resume consumption after restart
    So that messages are not lost or duplicated when subscribers restart

    Scenario: Delivery after restart using committed offset
        Given a publisher is configured with the following options:
            | Setting             | Value          |
            | Topic               | default        |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And a subscriber is configured with the following options:
            | Setting             | Value          |
            | Topic               | default        |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        When the publisher sends 6 messages to topic "default"
        Then subscriber C consumed messages up to offset 4
        When subscriber C restarts at offset 5
        Then a subscriber receives message "msg5" from topic "default"
        When the publisher sends message "resume-test" to topic "default"
        Then a subscriber receives message "resume-test" from topic "default"