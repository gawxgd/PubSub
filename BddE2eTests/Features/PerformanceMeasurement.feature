Feature: Performance Measurement
    As a system operator
    I want to measure message broker performance under various loads
    So that I can analyze throughput, latency, and reliability metrics

    @performance
    Scenario: Measure throughput at 100 msg/s for 30 seconds
        Given the scenario name is "throughput_100"
        And the report output directory is "perf_reports"
        And a publisher is configured with the following options:
            | Setting    | Value          |
            | Topic      | default        |
            | Broker     | 127.0.0.1:9096 |
            | Queue Size | 10000          |
        And a subscriber is configured with the following options:
            | Setting       | Value          |
            | Topic         | default        |
            | Broker        | 127.0.0.1:9098 |
            | Poll Interval | 50             |
        And a constant load of 100 messages per second for 30 seconds
        When the performance measurement is executed

    @performance
    Scenario: Measure throughput at 500 msg/s for 60 seconds
        Given the scenario name is "throughput_500"
        And the report output directory is "perf_reports"
        And a publisher is configured with the following options:
            | Setting    | Value          |
            | Topic      | test-topic     |
            | Broker     | 127.0.0.1:9096 |
            | Queue Size | 50000          |
        And a subscriber is configured with the following options:
            | Setting       | Value          |
            | Topic         | test-topic     |
            | Broker        | 127.0.0.1:9098 |
            | Poll Interval | 25             |
        And a constant load of 500 messages per second for 60 seconds
        When the performance measurement is executed

    @performance @longrun
    Scenario: Long-running stability test at 200 msg/s for 2 minutes
        Given the scenario name is "stability_2min"
        And the report output directory is "perf_reports"
        And a publisher is configured with the following options:
            | Setting    | Value          |
            | Topic      | custom-topic   |
            | Broker     | 127.0.0.1:9096 |
            | Queue Size | 100000         |
        And a subscriber is configured with the following options:
            | Setting       | Value          |
            | Topic         | custom-topic   |
            | Broker        | 127.0.0.1:9098 |
            | Poll Interval | 25             |
        And a constant load of 200 messages per second for 120 seconds
        When the performance measurement is executed

    @performance @lowrate
    Scenario: Measure latency at low rate (10 msg/s)
        Given the scenario name is "latency_low_rate"
        And the report output directory is "perf_reports"
        And a publisher is configured with the following options:
            | Setting    | Value          |
            | Topic      | default        |
            | Broker     | 127.0.0.1:9096 |
            | Queue Size | 1000           |
        And a subscriber is configured with the following options:
            | Setting       | Value          |
            | Topic         | default        |
            | Broker        | 127.0.0.1:9098 |
            | Poll Interval | 10             |
        And a constant load of 10 messages per second for 60 seconds
        When the performance measurement is executed
