Feature: Publishing messages to a non-existent topic

    Scenario: Publisher tries to publish a message to a topic which is not set up in the Commit Log
        Given a publisher is configured with the following options:
            | Setting             | Value          |
            | Topic               | non-existent   |
            | Broker              | 127.0.0.1:9096 |
            | Queue Size          | 1000           |
            | Max Retry Attempts  | 3              |
            | Max Send Attempts   | 3              |
        When the publisher sends message "message-content" to topic "non-existent"
        Then the publisher reports that the topic is not available
