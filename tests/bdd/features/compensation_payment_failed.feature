Feature: Saga compensation on payment failure
  Scenario: Payment fails after budget approval
    Given an OS is created with id "22222222-2222-2222-2222-222222222222"
    When the budget is generated and approved
    And the payment fails
    Then the saga should reach state "CANCEL_OS_REQUESTED" after budget cancellation
    And the saga should reach state "FAILED" after the OS is cancelled
