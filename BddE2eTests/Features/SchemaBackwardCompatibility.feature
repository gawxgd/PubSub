Feature: Schema backward compatibility - subscriber with newer schema reads messages from a publisher using old schema
    # Reading messages serialized with an older schema 
    
    Scenario: New schema has a new field with a default value 
        Given a publisher of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And a subscriber of type "TestEventWithAdditionalDefaultField" is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
        When the publisher sends a message 
            | Message |
            | p1      |
        Then the subscriber successfully receives 1 messages

    Scenario: A field has been removed from the new schema
        Given a publisher of type "TestEventWithAdditionalField" is configured with the following options:
            | Setting             | Value          |
            | topic               | test-topic     |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        And a subscriber of type "TestEvent" is configured with the following options:
            | Setting             | Value          |
            | Topic               | test-topic     |
            | Broker              | 127.0.0.1:9098 |
            | Poll Interval       | 100            |
            | Max Retry Attempts  | 3              |
         When the publisher sends a message 
            | Message |
            | p1      |
            | 5       |
        Then the subscriber receives message "p1" from topic "test-topic"
       