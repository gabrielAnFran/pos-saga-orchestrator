Feature: Service order happy path saga
  Scenario: OS is created, budget approved, payment confirmed, execution completes
    Given an OS is created with id "11111111-1111-1111-1111-111111111111"
    When the budget is generated and approved
    And the payment is confirmed
    And the execution starts and completes
    Then the saga should reach state "COMPLETED"
