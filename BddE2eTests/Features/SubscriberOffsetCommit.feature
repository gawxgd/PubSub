Feature: Restart Scenarios
As a system integrator
I want to verify that the system handles restarts correctly
So that messages are not lost or duplicated when components restart

    Scenario: Delivery after subscriber restart using committed offset
        Given a publisher is configured with the following options:
          | Setting            | Value          |
          | Topic              | default        |
          | Broker             | 127.0.0.1:9096 |
          | Queue Size         | 1000           |
          | Max Retry Attempts | 3              |
          | Max Send Attempts  | 3              |
        And a subscriber is configured with the following options:
          | Setting            | Value          |
          | Topic              | default        |
          | Broker             | 127.0.0.1:9098 |
          | Poll Interval      | 100            |
          | Max Retry Attempts | 3              |
        When the publisher sends 6 messages to topic "default"
        Then subscriber C consumed messages up to offset 4
        When subscriber C restarts at offset 5
        Then a subscriber receives message "msg5" from topic "default"
        When the publisher sends message "resume-test" to topic "default"
        Then a subscriber receives message "resume-test" from topic "default"

    Scenario: Delivery after broker restart
        Given a publisher is configured with the following options:
          | Setting            | Value          |
          | Topic              | default        |
          | Broker             | 127.0.0.1:9096 |
          | Queue Size         | 1000           |
          | Max Retry Attempts | 3              |
          | Max Send Attempts  | 3              |
        And a subscriber is configured with the following options:
          | Setting            | Value          |
          | Topic              | default        |
          | Broker             | 127.0.0.1:9098 |
          | Poll Interval      | 100            |
          | Max Retry Attempts | 3              |
        When the publisher sends 6 messages to topic "default"
        Then subscriber C consumed messages up to offset 4
        When the broker restarts
        Then a subscriber receives message "msg5" from topic "default"
        When the publisher sends message "resume-test" to topic "default"
        Then a subscriber receives message "resume-test" from topic "default"

#    Scenario: Reconnection after connection loss
#        Given a publisher is configured with the following options:
#            | Setting             | Value          |
#            | Topic               | default        |
#            | Broker              | 127.0.0.1:9096 |
#            | Queue Size          | 1000           |
#            | Max Retry Attempts  | 3              |
#            | Max Send Attempts   | 3              |
#        And a subscriber is configured with the following options:
#            | Setting             | Value          |
#            | Topic               | default        |
#            | Broker              | 127.0.0.1:9098 |
#            | Poll Interval       | 100            |
#            | Max Retry Attempts  | 3              |
#        When the publisher sends 3 messages to topic "default"
#        Then subscriber C consumed messages up to offset 2
#        When the subscriber connection is lost
#        And the subscriber reconnects
#        When the publisher sends message "resume-test" to topic "default"
#        Then a subscriber receives message "resume-test" from topic "default"