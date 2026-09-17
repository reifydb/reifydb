// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::value::{decode_params, encode_params};
use reifydb_value::{
	Result,
	params::Params,
	value::{Value, duration::Duration},
};
use serde::{Deserialize, Serialize};

use crate::{
	common::WindowKind,
	error::CoreError,
	internal_error,
	row::{JoinPick, JoinRetention},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WithSpan {
	Duration(Duration),
	Count(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowWith {
	pub kind: WindowKind,
	pub lateness: Option<Duration>,
	pub immutable: Option<Duration>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JoinWith {
	pub retention: Option<JoinRetention>,
	pub snapshot: bool,
	pub pick: Option<JoinPick>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyWith {
	pub lateness: Option<WithSpan>,
	pub immutable: Option<WithSpan>,
	pub retention: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateWith {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistinctWith {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowSealing {
	pub lateness: Option<Duration>,
	pub immutable: Option<Duration>,
}

impl WindowSealing {
	pub fn from_operator_with(with: &ApplyWith) -> Result<Self> {
		let sealing = Self {
			lateness: duration_span("lateness", with.lateness)?,
			immutable: duration_span("immutable", with.immutable)?,
		};
		if let (Some(lateness), Some(immutable)) = (sealing.lateness, sealing.immutable)
			&& immutable >= lateness
		{
			return Err(CoreError::OperatorWithImmutableNotBelowLateness {
				immutable,
				lateness,
			}
			.into());
		}
		Ok(sealing)
	}
}

pub fn encode_apply_with(with: &ApplyWith) -> Result<Vec<u8>> {
	let mut values = HashMap::new();
	if let Some(lateness) = with.lateness {
		values.insert("lateness".to_string(), span_value(lateness));
	}
	if let Some(immutable) = with.immutable {
		values.insert("immutable".to_string(), span_value(immutable));
	}
	if let Some(retention) = with.retention {
		values.insert("retention".to_string(), Value::Duration(retention));
	}
	encode_params(&Params::Named(Arc::new(values)))
		.map_err(|e| internal_error!("failed to encode apply with: {}", e))
}

pub fn decode_apply_with(bytes: &[u8]) -> Result<ApplyWith> {
	let values = match decode_params(bytes).map_err(|e| internal_error!("failed to decode apply with: {}", e))? {
		Params::None => return Ok(ApplyWith::default()),
		Params::Named(values) => values,
		Params::Positional(_) => return Err(internal_error!("apply with must be named params")),
	};
	let mut with = ApplyWith::default();
	for (key, value) in values.iter() {
		match (key.as_str(), value) {
			("lateness", value) => with.lateness = Some(value_span(key, value)?),
			("immutable", value) => with.immutable = Some(value_span(key, value)?),
			("retention", Value::Duration(retention)) => with.retention = Some(*retention),
			_ => return Err(internal_error!("unexpected apply with entry {}: {:?}", key, value)),
		}
	}
	Ok(with)
}

fn span_value(span: WithSpan) -> Value {
	match span {
		WithSpan::Duration(duration) => Value::Duration(duration),
		WithSpan::Count(count) => Value::Uint8(count),
	}
}

fn value_span(key: &str, value: &Value) -> Result<WithSpan> {
	match value {
		Value::Duration(duration) => Ok(WithSpan::Duration(*duration)),
		Value::Uint8(count) => Ok(WithSpan::Count(*count)),
		_ => Err(internal_error!("unexpected apply with entry {}: {:?}", key, value)),
	}
}

fn duration_span(key: &'static str, span: Option<WithSpan>) -> Result<Option<Duration>> {
	match span {
		None => Ok(None),
		Some(WithSpan::Duration(duration)) => Ok(Some(duration)),
		Some(WithSpan::Count(count)) => Err(CoreError::OperatorWithCountSpan {
			key,
			count,
		}
		.into()),
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, sync::Arc};

	use reifydb_codec::value::encode_params;
	use reifydb_value::{
		params::Params,
		value::{Value, duration::Duration},
	};

	use super::{ApplyWith, WindowSealing, WithSpan, decode_apply_with, encode_apply_with};

	fn secs(n: i64) -> Duration {
		Duration::from_seconds(n).unwrap()
	}

	fn with(lateness: Option<i64>, immutable: Option<i64>) -> ApplyWith {
		ApplyWith {
			lateness: lateness.map(|n| WithSpan::Duration(secs(n))),
			immutable: immutable.map(|n| WithSpan::Duration(secs(n))),
			retention: None,
		}
	}

	#[test]
	fn declared_immutable_below_lateness_is_kept_verbatim() {
		assert_eq!(
			WindowSealing::from_operator_with(&with(Some(20), Some(15))).unwrap(),
			WindowSealing {
				lateness: Some(secs(20)),
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn lateness_without_immutable_resolves_with_no_immutable() {
		// Substituting the lateness would arm the sealing slots on a window that never asked to seal.
		assert_eq!(
			WindowSealing::from_operator_with(&with(Some(20), None)).unwrap(),
			WindowSealing {
				lateness: Some(secs(20)),
				immutable: None,
			}
		);
	}

	#[test]
	fn declared_immutable_equal_to_lateness_is_rejected() {
		// The bound is strict; an immutable equal to the lateness would never seal before the window closes.
		assert!(WindowSealing::from_operator_with(&with(Some(20), Some(20))).is_err());
	}

	#[test]
	fn declared_immutable_above_lateness_is_rejected() {
		assert!(WindowSealing::from_operator_with(&with(Some(20), Some(30))).is_err());
	}

	#[test]
	fn immutable_without_lateness_is_accepted() {
		// The ordering bound needs both knobs, so an immutable alone must reach the operator untouched.
		assert_eq!(
			WindowSealing::from_operator_with(&with(None, Some(15))).unwrap(),
			WindowSealing {
				lateness: None,
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn neither_knob_declared_leaves_both_absent() {
		// An undeclared knob must stay absent, never resolve to zero, which is itself a legal declared value.
		assert_eq!(
			WindowSealing::from_operator_with(&with(None, None)).unwrap(),
			WindowSealing {
				lateness: None,
				immutable: None,
			}
		);
	}

	#[test]
	fn validated_returns_both_when_declared() {
		assert_eq!(
			WindowSealing::from_operator_with(&with(Some(20), Some(15))).unwrap(),
			WindowSealing {
				lateness: Some(secs(20)),
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn validated_returns_no_immutable_when_only_the_lateness_is_declared() {
		// A window under the immutable floor declares a lateness alone and must still resolve.
		assert_eq!(
			WindowSealing::from_operator_with(&with(Some(20), None)).unwrap(),
			WindowSealing {
				lateness: Some(secs(20)),
				immutable: None,
			}
		);
	}

	#[test]
	fn validated_names_the_ordering_violation() {
		let err = WindowSealing::from_operator_with(&with(Some(20), Some(20))).unwrap_err();
		assert!(err.to_string().contains("must be strictly less than lateness"), "{err}");
	}

	#[test]
	fn validated_accepts_an_immutable_without_a_lateness() {
		// Validation covers the ordering rule only; neither knob is required, so this must not fail.
		assert_eq!(
			WindowSealing::from_operator_with(&with(None, Some(15))).unwrap(),
			WindowSealing {
				lateness: None,
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn validated_accepts_a_window_that_declares_no_knob_at_all() {
		assert_eq!(
			WindowSealing::from_operator_with(&with(None, None)).unwrap(),
			WindowSealing {
				lateness: None,
				immutable: None,
			}
		);
	}

	#[test]
	fn a_count_span_is_rejected_for_a_time_sealed_operator() {
		// A count read as a duration would seal after that many nanoseconds instead of that many rows.
		let with = ApplyWith {
			lateness: Some(WithSpan::Count(150)),
			immutable: None,
			retention: None,
		};
		let err = WindowSealing::from_operator_with(&with).unwrap_err();
		assert!(err.to_string().contains("lateness"), "{err}");
	}

	#[test]
	fn apply_with_round_trips_through_the_create_buffer() {
		// A span that changes unit on the way to the guest seals after rows instead of time, or the reverse.
		for with in [
			ApplyWith::default(),
			with(Some(30), Some(10)),
			ApplyWith {
				lateness: Some(WithSpan::Count(150)),
				immutable: Some(WithSpan::Count(0)),
				retention: Some(secs(3600)),
			},
		] {
			assert_eq!(decode_apply_with(&encode_apply_with(&with).unwrap()).unwrap(), with);
		}
	}

	#[test]
	fn an_unknown_apply_with_key_fails_to_decode() {
		// Dropping a key the guest does not know would run the operator without a setting the author declared.
		let values = HashMap::from([("ttl".to_string(), Value::Duration(secs(1)))]);
		let bytes = encode_params(&Params::Named(Arc::new(values))).unwrap();
		assert!(decode_apply_with(&bytes).is_err());
	}

	#[test]
	fn a_span_of_the_wrong_type_fails_to_decode() {
		let values = HashMap::from([("lateness".to_string(), Value::Int8(5))]);
		let bytes = encode_params(&Params::Named(Arc::new(values))).unwrap();
		assert!(decode_apply_with(&bytes).is_err());
	}

	#[test]
	fn positional_params_fail_to_decode() {
		let bytes = encode_params(&Params::Positional(Arc::new(vec![Value::Duration(secs(1))]))).unwrap();
		assert!(decode_apply_with(&bytes).is_err());
	}
}
