#![cfg(feature = "wire")]

use jiff::SignedDuration;
use proptest::prelude::*;
use serde_json::json;
use tollgate_core::{AccountId, CostUnits, FencingToken, LeaseId};
use tollgate_store::AllocateError;
use tollgate_store::wire::{AcquireRequest, ConsolidateRequest, LeaseTtl};

#[test]
fn whole_second_ttls_keep_the_legacy_wire_contract() {
    for seconds in [1, 60, u32::MAX] {
        let ttl = SignedDuration::from_secs(i64::from(seconds));
        let encoded = serde_json::to_value(LeaseTtl::try_from(ttl).unwrap()).unwrap();
        assert_eq!(encoded, json!({"ttl_seconds": seconds}));
        assert_eq!(
            serde_json::from_value::<LeaseTtl>(encoded)
                .unwrap()
                .duration(),
            Ok(ttl)
        );
    }
}

#[test]
fn precise_ttls_carry_an_exact_string_and_a_refusing_legacy_sentinel() {
    for ttl in [
        SignedDuration::from_nanos(1),
        SignedDuration::from_millis(500),
        SignedDuration::from_millis(1_500),
        SignedDuration::from_secs(i64::from(u32::MAX) + 1),
        SignedDuration::MAX,
    ] {
        let encoded = serde_json::to_value(LeaseTtl::try_from(ttl).unwrap()).unwrap();
        assert_eq!(encoded["ttl_seconds"], 0);
        let exact: SignedDuration = encoded["ttl"].as_str().unwrap().parse().unwrap();
        assert_eq!(exact, ttl);
        assert_eq!(
            serde_json::from_value::<LeaseTtl>(encoded)
                .unwrap()
                .duration(),
            Ok(ttl)
        );
    }
}

#[test]
fn nonpositive_and_ambiguous_ttls_are_rejected() {
    for ttl in [
        SignedDuration::ZERO,
        SignedDuration::from_nanos(-1),
        SignedDuration::from_secs(-1),
        SignedDuration::MIN,
    ] {
        assert!(matches!(
            LeaseTtl::try_from(ttl),
            Err(AllocateError::InvalidTtl)
        ));
        let wire = json!({"ttl_seconds": 0, "ttl": ttl});
        assert_eq!(
            serde_json::from_value::<LeaseTtl>(wire).unwrap().duration(),
            Err(AllocateError::InvalidTtl)
        );
    }
    for wire in [
        json!({"ttl_seconds": 0}),
        json!({"ttl_seconds": 0, "ttl": null}),
        json!({"ttl_seconds": 60, "ttl": "PT0.5S"}),
        json!({"ttl_seconds": 60, "ttl": "PT60S"}),
    ] {
        assert_eq!(
            serde_json::from_value::<LeaseTtl>(wire).unwrap().duration(),
            Err(AllocateError::InvalidTtl)
        );
    }
}

#[test]
fn malformed_or_partial_ttl_fields_are_not_repaired() {
    for wire in [
        r#"{}"#,
        r#"{"ttl":"PT0.5S"}"#,
        r#"{"ttl_seconds":null,"ttl":"PT0.5S"}"#,
        r#"{"ttl_seconds":-1}"#,
        r#"{"ttl_seconds":1.5}"#,
        r#"{"ttl_seconds":4294967296}"#,
        r#"{"ttl_seconds":0,"ttl":0.5}"#,
        r#"{"ttl_seconds":0,"ttl":"not-a-duration"}"#,
        r#"{"ttl_seconds":1,"ttl_seconds":2}"#,
        r#"{"ttl_seconds":0,"ttl":"PT1S","ttl":"PT2S"}"#,
    ] {
        assert!(serde_json::from_str::<LeaseTtl>(wire).is_err(), "{wire}");
    }
}

#[test]
fn duplicated_ttl_fields_are_rejected_inside_both_request_envelopes() {
    for ttl_fields in [
        r#""ttl_seconds":1,"ttl_seconds":2"#,
        r#""ttl_seconds":0,"ttl":"PT1S","ttl":"PT2S"#,
    ] {
        let acquire = format!(
            r#"{{"account_id":"00000000000000000000000000000001","requested":10,{ttl_fields}}}"#
        );
        assert!(serde_json::from_str::<AcquireRequest>(&acquire).is_err());
        let consolidate = format!(
            r#"{{"lease_id":"00000000000000000000000000000001","fencing_token":1,"unspent":10,"requested":10,{ttl_fields}}}"#
        );
        assert!(serde_json::from_str::<ConsolidateRequest>(&consolidate).is_err());
    }
}

proptest! {
    #[test]
    fn every_positive_duration_round_trips_in_both_lease_requests(
        seconds in 0i64..=i64::MAX,
        nanos in 0i32..1_000_000_000,
    ) {
        prop_assume!(seconds != 0 || nanos != 0);
        let duration = SignedDuration::new(seconds, nanos);
        let ttl = LeaseTtl::try_from(duration).unwrap();
        let acquire = AcquireRequest {
            account_id: AccountId(1), requested: CostUnits(10), ttl,
        };
        let acquire: AcquireRequest =
            serde_json::from_slice(&serde_json::to_vec(&acquire).unwrap()).unwrap();
        prop_assert_eq!(acquire.ttl.duration(), Ok(duration));

        let consolidate = ConsolidateRequest {
            lease_id: LeaseId(1), fencing_token: FencingToken(1),
            unspent: CostUnits(10), requested: CostUnits(10), ttl,
        };
        let consolidate: ConsolidateRequest =
            serde_json::from_slice(&serde_json::to_vec(&consolidate).unwrap()).unwrap();
        prop_assert_eq!(consolidate.ttl.duration(), Ok(duration));
    }
}
