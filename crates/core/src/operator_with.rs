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
	common::{WindowKind, WindowSize},
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyWith {
	pub window: Option<WindowKind>,
	pub lateness: Option<WithSpan>,
	pub immutable: Option<WithSpan>,
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

impl ApplyWith {
	pub fn window_duration(&self) -> Result<Duration> {
		let Some(kind) = &self.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		match kind.size() {
			None => Err(CoreError::OperatorWithWindowKindUnsupported {
				kind: kind.name(),
				supported: "tumbling, sliding or rolling",
			}
			.into()),
			Some(WindowSize::Duration(d)) => Ok(*d),
			Some(WindowSize::Count(count)) => Err(CoreError::OperatorWithWindowSizeCount {
				count: *count,
			}
			.into()),
		}
	}

	pub fn window_slots(&self) -> Result<u64> {
		let Some(kind) = &self.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		match kind.size() {
			None => Err(CoreError::OperatorWithWindowKindUnsupported {
				kind: kind.name(),
				supported: "tumbling, sliding or rolling",
			}
			.into()),
			Some(WindowSize::Count(n)) => Ok(*n),
			Some(WindowSize::Duration(d)) => Err(CoreError::OperatorWithWindowSizeDuration {
				size: *d,
			}
			.into()),
		}
	}

	pub fn window_slide_duration(&self) -> Result<Option<Duration>> {
		match &self.window {
			Some(WindowKind::Sliding {
				slide,
				..
			}) => match slide {
				WindowSize::Duration(d) => Ok(Some(*d)),
				WindowSize::Count(count) => Err(CoreError::OperatorWithWindowSlideCount {
					count: *count,
				}
				.into()),
			},
			_ => Ok(None),
		}
	}

	pub fn window_slide_slots(&self) -> Result<Option<u64>> {
		match &self.window {
			Some(WindowKind::Sliding {
				slide,
				..
			}) => match slide {
				WindowSize::Count(n) => Ok(Some(*n)),
				WindowSize::Duration(d) => Err(CoreError::OperatorWithWindowSlideDuration {
					slide: *d,
				}
				.into()),
			},
			_ => Ok(None),
		}
	}

	pub fn reject_window(&self) -> Result<()> {
		match &self.window {
			None => Ok(()),
			Some(kind) => Err(CoreError::OperatorWithWindowNotSupported {
				kind: kind.name(),
			}
			.into()),
		}
	}

	pub fn require_window(&self, supported: &'static str) -> Result<&WindowKind> {
		let Some(kind) = &self.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		if kind.name() != supported {
			return Err(CoreError::OperatorWithWindowKindUnsupported {
				kind: kind.name(),
				supported,
			}
			.into());
		}
		Ok(kind)
	}

	pub fn lateness_duration(&self) -> Result<Option<Duration>> {
		match self.lateness {
			None => Ok(None),
			Some(WithSpan::Duration(d)) => Ok(Some(d)),
			Some(WithSpan::Count(count)) => Err(CoreError::OperatorWithCountSpan {
				key: "lateness",
				count,
			}
			.into()),
		}
	}

	pub fn lateness_count(&self) -> Result<Option<u64>> {
		match self.lateness {
			None => Ok(None),
			Some(WithSpan::Count(n)) => Ok(Some(n)),
			Some(WithSpan::Duration(duration)) => Err(CoreError::OperatorWithDurationSpan {
				key: "lateness",
				duration,
			}
			.into()),
		}
	}

	pub fn immutable_count(&self) -> Result<Option<u64>> {
		match self.immutable {
			None => Ok(None),
			Some(WithSpan::Count(n)) => Ok(Some(n)),
			Some(WithSpan::Duration(duration)) => Err(CoreError::OperatorWithDurationSpan {
				key: "immutable",
				duration,
			}
			.into()),
		}
	}
}

pub fn encode_apply_with(with: &ApplyWith) -> Result<Vec<u8>> {
	let mut values = HashMap::new();
	if let Some(window) = &with.window {
		values.insert("window".to_string(), Value::Utf8(window.name().to_string()));
		match window {
			WindowKind::Tumbling {
				size,
			} => insert_window_size(&mut values, size),
			WindowKind::Sliding {
				size,
				slide,
			} => {
				insert_window_size(&mut values, size);
				values.insert("slide".to_string(), window_size_value(slide));
			}
			WindowKind::Rolling {
				size,
				lag,
				pane,
			} => {
				insert_window_size(&mut values, size);
				if let Some(lag) = lag {
					values.insert("lag".to_string(), Value::Duration(*lag));
				}
				if let Some(pane) = pane {
					values.insert("pane".to_string(), Value::Duration(*pane));
				}
			}
			WindowKind::Session {
				gap,
			} => {
				values.insert("gap".to_string(), Value::Duration(*gap));
			}
		}
	}
	if let Some(lateness) = with.lateness {
		values.insert("lateness".to_string(), span_value(lateness));
	}
	if let Some(immutable) = with.immutable {
		values.insert("immutable".to_string(), span_value(immutable));
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
	let mut window_name: Option<String> = None;
	let mut duration: Option<Duration> = None;
	let mut slots: Option<u64> = None;
	let mut slide: Option<WindowSize> = None;
	let mut gap: Option<Duration> = None;
	let mut lag: Option<Duration> = None;
	let mut pane: Option<Duration> = None;
	for (key, value) in values.iter() {
		match (key.as_str(), value) {
			("window", Value::Utf8(name)) => window_name = Some(name.clone()),
			("duration", Value::Duration(d)) => duration = Some(*d),
			("slots", Value::Uint8(n)) => slots = Some(*n),
			("slide", Value::Duration(d)) => slide = Some(WindowSize::Duration(*d)),
			("slide", Value::Uint8(n)) => slide = Some(WindowSize::Count(*n)),
			("gap", Value::Duration(d)) => gap = Some(*d),
			("lag", Value::Duration(d)) => lag = Some(*d),
			("pane", Value::Duration(d)) => pane = Some(*d),
			("lateness", value) => with.lateness = Some(value_span(key, value)?),
			("immutable", value) => with.immutable = Some(value_span(key, value)?),
			_ => return Err(internal_error!("unexpected apply with entry {}: {:?}", key, value)),
		}
	}
	with.window = decode_window_kind(window_name, duration, slots, slide, gap, lag, pane)?;
	Ok(with)
}

fn decode_window_kind(
	window_name: Option<String>,
	duration: Option<Duration>,
	slots: Option<u64>,
	slide: Option<WindowSize>,
	gap: Option<Duration>,
	lag: Option<Duration>,
	pane: Option<Duration>,
) -> Result<Option<WindowKind>> {
	let Some(window_name) = window_name else {
		if duration.is_some()
			|| slots.is_some() || slide.is_some()
			|| gap.is_some() || lag.is_some()
			|| pane.is_some()
		{
			return Err(internal_error!("apply with has a window size key without 'window'"));
		}
		return Ok(None);
	};
	match window_name.as_str() {
		"tumbling" => {
			if slide.is_some() || gap.is_some() || lag.is_some() || pane.is_some() {
				return Err(internal_error!("apply with has a key the tumbling window does not use"));
			}
			Ok(Some(WindowKind::Tumbling {
				size: decode_window_size(duration, slots)?,
			}))
		}
		"sliding" => {
			let size = decode_window_size(duration, slots)?;
			let Some(slide) = slide else {
				return Err(internal_error!("apply with is missing 'slide' for a sliding window"));
			};
			if gap.is_some() || lag.is_some() || pane.is_some() {
				return Err(internal_error!("apply with has a key the sliding window does not use"));
			}
			Ok(Some(WindowKind::Sliding {
				size,
				slide,
			}))
		}
		"rolling" => {
			let size = decode_window_size(duration, slots)?;
			if slide.is_some() || gap.is_some() {
				return Err(internal_error!("apply with has a key the rolling window does not use"));
			}
			Ok(Some(WindowKind::Rolling {
				size,
				lag,
				pane,
			}))
		}
		"session" => {
			let Some(gap) = gap else {
				return Err(internal_error!("apply with is missing 'gap' for a session window"));
			};
			if duration.is_some() || slots.is_some() || slide.is_some() || lag.is_some() || pane.is_some() {
				return Err(internal_error!("apply with has a key the session window does not use"));
			}
			Ok(Some(WindowKind::Session {
				gap,
			}))
		}
		other => Err(internal_error!("apply with has an unknown window kind {}", other)),
	}
}

fn decode_window_size(duration: Option<Duration>, slots: Option<u64>) -> Result<WindowSize> {
	match (duration, slots) {
		(Some(d), None) => Ok(WindowSize::Duration(d)),
		(None, Some(n)) => Ok(WindowSize::Count(n)),
		(None, None) => Err(internal_error!("apply with is missing 'duration' or 'slots' for its window")),
		(Some(_), Some(_)) => Err(internal_error!("apply with has both 'duration' and 'slots'")),
	}
}

fn insert_window_size(values: &mut HashMap<String, Value>, size: &WindowSize) {
	match size {
		WindowSize::Duration(d) => {
			values.insert("duration".to_string(), Value::Duration(*d));
		}
		WindowSize::Count(n) => {
			values.insert("slots".to_string(), Value::Uint8(*n));
		}
	}
}

fn window_size_value(size: &WindowSize) -> Value {
	match size {
		WindowSize::Duration(d) => Value::Duration(*d),
		WindowSize::Count(n) => Value::Uint8(*n),
	}
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

	use super::{ApplyWith, WindowKind, WindowSealing, WindowSize, WithSpan, decode_apply_with, encode_apply_with};

	fn secs(n: i64) -> Duration {
		Duration::from_seconds(n).unwrap()
	}

	fn with(lateness: Option<i64>, immutable: Option<i64>) -> ApplyWith {
		ApplyWith {
			window: None,
			lateness: lateness.map(|n| WithSpan::Duration(secs(n))),
			immutable: immutable.map(|n| WithSpan::Duration(secs(n))),
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
			window: None,
			lateness: Some(WithSpan::Count(150)),
			immutable: None,
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
				window: None,
				lateness: Some(WithSpan::Count(150)),
				immutable: Some(WithSpan::Count(0)),
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

	#[test]
	fn every_window_kind_round_trips_through_the_create_buffer() {
		// A kind that decodes into the wrong shape would run the guest with a size or slide in the wrong unit.
		let lateness = Some(WithSpan::Duration(secs(30)));
		let kinds = [
			WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			},
			WindowKind::Tumbling {
				size: WindowSize::Count(10),
			},
			WindowKind::Sliding {
				size: WindowSize::Duration(secs(60)),
				slide: WindowSize::Duration(secs(30)),
			},
			WindowKind::Sliding {
				size: WindowSize::Count(10),
				slide: WindowSize::Count(5),
			},
			WindowKind::Rolling {
				size: WindowSize::Duration(secs(60)),
				lag: Some(secs(5)),
				pane: None,
			},
			WindowKind::Rolling {
				size: WindowSize::Duration(secs(60)),
				lag: None,
				pane: None,
			},
			WindowKind::Session {
				gap: secs(30),
			},
		];
		for kind in kinds {
			let with = ApplyWith {
				window: Some(kind),
				lateness,
				immutable: None,
			};
			assert_eq!(decode_apply_with(&encode_apply_with(&with).unwrap()).unwrap(), with);
		}
	}

	#[test]
	fn a_size_key_without_a_window_fails_to_decode() {
		// A size with no 'window' has nothing to build a WindowKind against.
		let values = HashMap::from([("duration".to_string(), Value::Duration(secs(60)))]);
		let bytes = encode_params(&Params::Named(Arc::new(values))).unwrap();
		assert!(decode_apply_with(&bytes).is_err());
	}

	#[test]
	fn window_duration_rejects_a_missing_window_a_count_size_and_a_session() {
		assert!(ApplyWith::default().window_duration().is_err());

		let count = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: None,
			immutable: None,
		};
		assert!(count.window_duration().is_err());

		let session = ApplyWith {
			window: Some(WindowKind::Session {
				gap: secs(30),
			}),
			lateness: None,
			immutable: None,
		};
		assert!(session.window_duration().is_err());
	}

	#[test]
	fn window_slots_rejects_a_missing_window_and_a_duration_size() {
		assert!(ApplyWith::default().window_slots().is_err());

		let duration = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			lateness: None,
			immutable: None,
		};
		assert!(duration.window_slots().is_err());
	}

	#[test]
	fn window_slide_reads_only_a_sliding_window_and_only_in_its_unit() {
		// a slide read from another kind or in the wrong unit would size every sliding window wrong with no
		// error
		let by_time = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Duration(secs(60)),
				slide: WindowSize::Duration(secs(15)),
			}),
			lateness: None,
			immutable: None,
		};
		let by_slots = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Count(10),
				slide: WindowSize::Count(4),
			}),
			lateness: None,
			immutable: None,
		};
		let tumbling = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			lateness: None,
			immutable: None,
		};

		assert_eq!(by_time.window_slide_duration().unwrap(), Some(secs(15)));
		assert_eq!(by_slots.window_slide_slots().unwrap(), Some(4));
		assert!(by_time.window_slide_slots().is_err());
		assert!(by_slots.window_slide_duration().is_err());
		assert_eq!(tumbling.window_slide_duration().unwrap(), None);
		assert_eq!(tumbling.window_slide_slots().unwrap(), None);
	}

	#[test]
	fn reject_window_refuses_any_window() {
		assert!(ApplyWith::default().reject_window().is_ok());

		let windowed = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			lateness: None,
			immutable: None,
		};
		assert!(windowed.reject_window().is_err());
	}

	#[test]
	fn require_window_refuses_a_missing_window_and_another_kind() {
		assert!(ApplyWith::default().require_window("tumbling").is_err());

		let rolling = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(secs(60)),
				lag: None,
				pane: None,
			}),
			lateness: None,
			immutable: None,
		};
		assert!(rolling.require_window("tumbling").is_err());
		assert!(rolling.require_window("rolling").is_ok());
	}

	#[test]
	fn lateness_readers_refuse_the_other_unit() {
		let none = ApplyWith::default();
		assert_eq!(none.lateness_duration().unwrap(), None);
		assert_eq!(none.lateness_count().unwrap(), None);

		let duration = ApplyWith {
			window: None,
			lateness: Some(WithSpan::Duration(secs(30))),
			immutable: None,
		};
		assert_eq!(duration.lateness_duration().unwrap(), Some(secs(30)));
		assert!(duration.lateness_count().is_err());

		let count = ApplyWith {
			window: None,
			lateness: Some(WithSpan::Count(5)),
			immutable: None,
		};
		assert_eq!(count.lateness_count().unwrap(), Some(5));
		assert!(count.lateness_duration().is_err());
	}

	#[test]
	fn a_rolling_pane_round_trips_through_the_create_buffer() {
		// A pane lost or moved between the reader and the guest would run the window at the wrong resolution.
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(secs(3600)),
				lag: Some(secs(5)),
				pane: Some(secs(1)),
			}),
			lateness: None,
			immutable: None,
		};
		assert_eq!(decode_apply_with(&encode_apply_with(&with).unwrap()).unwrap(), with);
	}

	#[test]
	fn a_pane_on_a_window_that_is_not_rolling_fails_to_decode() {
		// A pane the kind never reads is a setting the author believes is in force.
		for (window, extra) in [
			("tumbling", ("duration", Value::Duration(secs(60)))),
			("sliding", ("duration", Value::Duration(secs(60)))),
			("session", ("gap", Value::Duration(secs(30)))),
		] {
			let mut values = HashMap::from([
				("window".to_string(), Value::Utf8(window.to_string())),
				("pane".to_string(), Value::Duration(secs(1))),
				(extra.0.to_string(), extra.1),
			]);
			if window == "sliding" {
				values.insert("slide".to_string(), Value::Duration(secs(30)));
			}
			let bytes = encode_params(&Params::Named(Arc::new(values))).unwrap();
			assert!(decode_apply_with(&bytes).is_err(), "pane must be rejected on {window}");
		}
	}

	#[test]
	fn a_pane_without_a_window_fails_to_decode() {
		let values = HashMap::from([("pane".to_string(), Value::Duration(secs(1)))]);
		let bytes = encode_params(&Params::Named(Arc::new(values))).unwrap();
		assert!(decode_apply_with(&bytes).is_err());
	}

	#[test]
	fn immutable_count_refuses_a_duration_and_reads_a_count() {
		// A duration read as a count would seal after that many nanoseconds' worth of rows.
		let none = ApplyWith::default();
		assert_eq!(none.immutable_count().unwrap(), None);

		let count = ApplyWith {
			window: None,
			lateness: None,
			immutable: Some(WithSpan::Count(4)),
		};
		assert_eq!(count.immutable_count().unwrap(), Some(4));

		let duration = ApplyWith {
			window: None,
			lateness: None,
			immutable: Some(WithSpan::Duration(secs(4))),
		};
		assert!(duration.immutable_count().is_err());
	}
}
