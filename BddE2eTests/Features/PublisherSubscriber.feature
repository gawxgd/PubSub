Feature: Publisher and Subscriber Communication
    As a system integrator
    I want to verify that publishers can send messages and subscribers can receive them
    So that the message broker correctly routes messages between components

    Scenario: Publisher sends message and subscriber receives it
        Given a publisher sends message "Hello World" to topic "test-topic"
        Then a subscriber receives message "Hello World" from topic "test-topic"
