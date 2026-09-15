#![cfg(feature = "wire")]

use serde_json::json;
use tollgate_store::wire::SetBudgetRequest;

#[test]
fn a_budget_mutation_requires_explicit_field_presence() {
    for value in [
        json!({}),
        json!({"allowance": 100}),
        json!({"budget": false}),
        json!(null),
    ] {
        assert!(serde_json::from_value::<SetBudgetRequest>(value).is_err());
    }
}

#[test]
fn explicit_null_clears_while_zero_remains_a_budget_schedule() {
    let clear: SetBudgetRequest = serde_json::from_value(json!({"budget": null})).unwrap();
    assert_eq!(clear.budget, None);
    assert_eq!(
        serde_json::to_value(clear).unwrap(),
        json!({"budget": null})
    );
    let zero =
        json!({"budget": {"allowance": 0, "period": "UtcCalendarMonth", "rollover": "None"}});
    let request: SetBudgetRequest = serde_json::from_value(zero.clone()).unwrap();
    assert_eq!(request.budget.unwrap().allowance.get(), 0);
    assert_eq!(serde_json::to_value(request).unwrap(), zero);
}
