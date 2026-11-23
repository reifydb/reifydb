// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::value::{decode_params, encode_params};
use reifydb_value::{
	error::ValueError,
	params::Params,
	value::{Value, duration::Duration},
};
use serde::{Deserialize, Serialize};

use crate::{
	common::{OnEmpty, WindowKind, WindowSize},
	error::CoreError,
	internal_error,
	row::{JoinPick, JoinSpans},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WithSpan {
	Duration(Duration),
	Count(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowWith {
	pub kind: WindowKind,
	pub drop: Option<Duration>,
	pub immutable: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Emit {
	#[default]
	On,
	Off,
	After(Duration),
}

impl From<bool> for Emit {
	fn from(on: bool) -> Self {
		match on {
			true => Emit::On,
			false => Emit::Off,
		}
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum InsertTo {
	#[default]
	Insert,
	Drop,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateTo {
	#[default]
	Update,
	Insert,
	Drop,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteTo {
	#[default]
	Delete,
	Insert,
	Drop,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangesWith {
	pub insert: InsertTo,
	pub update: UpdateTo,
	pub update_on: Vec<String>,
	pub delete: DeleteTo,
}

impl ChangesWith {
	pub fn renumbers_inserts(&self) -> bool {
		self.update == UpdateTo::Insert || (self.update == UpdateTo::Drop && self.delete == DeleteTo::Insert)
	}
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct JoinWith {
	pub drop: Option<JoinSpans>,
	pub forget: Option<JoinSpans>,
	pub emit: Emit,
	pub pick: Option<JoinPick>,
	pub exact: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupWith {
	pub drop: Option<Duration>,
	pub forget: Option<Duration>,
	pub emit: Emit,
	pub exact: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyWith {
	pub window: Option<WindowKind>,
	pub drop: Option<WithSpan>,
	pub immutable: Option<WithSpan>,
	pub forget: Option<Duration>,
}

fn check_forget_not_below_drop(forget: Option<Duration>, drop_span: Option<Duration>) -> Result<(), ValueError> {
	let (Some(forget), Some(drop_span)) = (forget, drop_span) else {
		return Ok(());
	};
	if forget < drop_span {
		return Err(CoreError::OperatorWithForgetBelowDrop {
			forget,
			drop: drop_span,
		}
		.into());
	}
	Ok(())
}

impl JoinWith {
	pub fn forget_left(&self) -> Option<Duration> {
		self.forget.as_ref().and_then(|spans| spans.left)
	}

	pub fn forget_right(&self) -> Option<Duration> {
		self.forget.as_ref().and_then(|spans| spans.right)
	}

	pub fn effective_drop_left(&self) -> Option<Duration> {
		self.drop.as_ref().and_then(|spans| spans.left).or(self.forget_left())
	}

	pub fn effective_drop_right(&self) -> Option<Duration> {
		self.drop.as_ref().and_then(|spans| spans.right).or(self.forget_right())
	}

	pub fn check_forget(&self) -> Result<(), ValueError> {
		check_forget_not_below_drop(self.forget_left(), self.drop.as_ref().and_then(|spans| spans.left))?;
		check_forget_not_below_drop(self.forget_right(), self.drop.as_ref().and_then(|spans| spans.right))
	}
}

impl LookupWith {
	pub fn effective_drop(&self) -> Option<Duration> {
		self.drop.or(self.forget)
	}

	pub fn check_forget(&self) -> Result<(), ValueError> {
		check_forget_not_below_drop(self.forget, self.drop)
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateWith {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistinctWith {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowKnobs {
	pub drop: Option<Duration>,
	pub immutable: Option<Duration>,
}

impl WindowKnobs {
	pub fn from_operator_with(with: &ApplyWith) -> Result<Self, ValueError> {
		let knobs = Self {
			drop: duration_span("drop", with.drop)?,
			immutable: duration_span("immutable", with.immutable)?,
		};
		if let (Some(drop_span), Some(immutable)) = (knobs.drop, knobs.immutable)
			&& immutable >= drop_span
		{
			return Err(CoreError::OperatorWithImmutableNotBelowDrop {
				immutable,
				drop: drop_span,
			}
			.into());
		}
		Ok(knobs)
	}
}

impl ApplyWith {
	pub fn window_duration(&self) -> Result<Duration, ValueError> {
		let Some(kind) = &self.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		match kind.size() {
			None => Err(CoreError::OperatorWithWindowKindUnsupported {
				kind: kind.name(),
				supported: "tumbling, sliding or rolling".to_string(),
			}
			.into()),
			Some(WindowSize::Duration(d)) => Ok(*d),
			Some(WindowSize::Count(count)) => Err(CoreError::OperatorWithWindowSizeCount {
				count: *count,
			}
			.into()),
		}
	}

	pub fn window_slots(&self) -> Result<u64, ValueError> {
		let Some(kind) = &self.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		match kind.size() {
			None => Err(CoreError::OperatorWithWindowKindUnsupported {
				kind: kind.name(),
				supported: "tumbling, sliding or rolling".to_string(),
			}
			.into()),
			Some(WindowSize::Count(n)) => Ok(*n),
			Some(WindowSize::Duration(d)) => Err(CoreError::OperatorWithWindowSizeDuration {
				size: *d,
			}
			.into()),
		}
	}

	pub fn window_slide_duration(&self) -> Result<Option<Duration>, ValueError> {
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

	pub fn window_slide_slots(&self) -> Result<Option<u64>, ValueError> {
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

	pub fn window_session_gap(&self) -> Option<Duration> {
		match &self.window {
			Some(WindowKind::Session {
				gap,
			}) => Some(*gap),
			_ => None,
		}
	}

	pub fn check_session_window(&self) -> Result<(), ValueError> {
		let Some(gap) = self.window_session_gap() else {
			return Ok(());
		};
		if !gap.is_positive() {
			return Err(CoreError::OperatorWithSessionZeroGap.into());
		}
		self.drop_duration()?;
		match duration_span("immutable", self.immutable)? {
			Some(immutable) if immutable.is_positive() => Err(CoreError::OperatorWithSessionImmutable {
				immutable,
			}
			.into()),
			_ => Ok(()),
		}
	}

	pub fn effective_drop(&self) -> Result<Option<Duration>, ValueError> {
		match self.drop_duration()? {
			Some(drop_span) => Ok(Some(drop_span)),
			None => Ok(self.forget),
		}
	}

	pub fn check_forget(&self) -> Result<(), ValueError> {
		check_forget_not_below_drop(self.forget, self.drop_duration()?)
	}

	pub fn reject_forget(&self) -> Result<(), ValueError> {
		match self.forget {
			None => Ok(()),
			Some(_) => Err(CoreError::OperatorWithForgetNotSupported.into()),
		}
	}
	pub fn reject_window(&self) -> Result<(), ValueError> {
		match &self.window {
			None => Ok(()),
			Some(kind) => Err(CoreError::OperatorWithWindowNotSupported {
				kind: kind.name(),
			}
			.into()),
		}
	}

	pub fn require_window(&self, supported: &'static str) -> Result<&WindowKind, ValueError> {
		let Some(kind) = &self.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		if kind.name() != supported {
			return Err(CoreError::OperatorWithWindowKindUnsupported {
				kind: kind.name(),
				supported: supported.to_string(),
			}
			.into());
		}
		Ok(kind)
	}

	pub fn drop_duration(&self) -> Result<Option<Duration>, ValueError> {
		match self.drop {
			None => Ok(None),
			Some(WithSpan::Duration(d)) => Ok(Some(d)),
			Some(WithSpan::Count(count)) => Err(CoreError::OperatorWithCountSpan {
				key: "drop",
				count,
			}
			.into()),
		}
	}

	pub fn drop_count(&self) -> Result<Option<u64>, ValueError> {
		match self.drop {
			None => Ok(None),
			Some(WithSpan::Count(n)) => Ok(Some(n)),
			Some(WithSpan::Duration(duration)) => Err(CoreError::OperatorWithDurationSpan {
				key: "drop",
				duration,
			}
			.into()),
		}
	}

	pub fn immutable_count(&self) -> Result<Option<u64>, ValueError> {
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

pub fn encode_apply_with(with: &ApplyWith) -> Result<Vec<u8>, ValueError> {
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
				on_empty,
			} => {
				match on_empty {
					OnEmpty::Remove => {}
					OnEmpty::Carry => {
						values.insert("on_empty".to_string(), Value::Utf8("carry".to_string()));
					}
					OnEmpty::Zero => {
						return Err(internal_error!(
							"apply with cannot carry the rolling on empty zero"
						));
					}
				}
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
	if let Some(drop_span) = with.drop {
		values.insert("drop".to_string(), span_value(drop_span));
	}
	if let Some(immutable) = with.immutable {
		values.insert("immutable".to_string(), span_value(immutable));
	}
	if let Some(forget) = with.forget {
		values.insert("forget".to_string(), Value::Duration(forget));
	}
	encode_params(&Params::Named(Arc::new(values)))
		.map_err(|e| internal_error!("failed to encode apply with: {}", e))
}

pub fn decode_apply_with(bytes: &[u8]) -> Result<ApplyWith, ValueError> {
	let values = match decode_params(bytes).map_err(|e| internal_error!("failed to decode apply with: {}", e))? {
		Params::None => return Ok(ApplyWith::default()),
		Params::Named(values) => values,
		Params::Ordinal(_) => return Err(internal_error!("apply with must be named params")),
	};
	let mut with = ApplyWith::default();
	let mut window_name: Option<String> = None;
	let mut duration: Option<Duration> = None;
	let mut slots: Option<u64> = None;
	let mut slide: Option<WindowSize> = None;
	let mut gap: Option<Duration> = None;
	let mut lag: Option<Duration> = None;
	let mut pane: Option<Duration> = None;
	let mut on_empty = OnEmpty::Remove;
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
			("on_empty", Value::Utf8(action)) if action == "carry" => on_empty = OnEmpty::Carry,
			("drop", value) => with.drop = Some(value_span(key, value)?),
			("immutable", value) => with.immutable = Some(value_span(key, value)?),
			("forget", Value::Duration(forget)) => with.forget = Some(*forget),
			_ => return Err(internal_error!("unexpected apply with entry {}: {:?}", key, value)),
		}
	}
	with.window = decode_window_kind(window_name, duration, slots, slide, gap, lag, pane, on_empty)?;
	Ok(with)
}

#[allow(clippy::too_many_arguments)]
fn decode_window_kind(
	window_name: Option<String>,
	duration: Option<Duration>,
	slots: Option<u64>,
	slide: Option<WindowSize>,
	gap: Option<Duration>,
	lag: Option<Duration>,
	pane: Option<Duration>,
	on_empty: OnEmpty,
) -> Result<Option<WindowKind>, ValueError> {
	let Some(window_name) = window_name else {
		if duration.is_some()
			|| slots.is_some() || slide.is_some()
			|| gap.is_some() || lag.is_some()
			|| pane.is_some()
		{
			return Err(internal_error!("apply with has a window size key without 'window'"));
		}
		if on_empty != OnEmpty::Remove {
			return Err(internal_error!("apply with has 'on_empty' without 'window'"));
		}
		return Ok(None);
	};
	if on_empty != OnEmpty::Remove && window_name != "rolling" {
		return Err(internal_error!("apply with has 'on_empty' on a {} window", window_name));
	}
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
				on_empty,
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

fn decode_window_size(duration: Option<Duration>, slots: Option<u64>) -> Result<WindowSize, ValueError> {
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

fn value_span(key: &str, value: &Value) -> Result<WithSpan, ValueError> {
	match value {
		Value::Duration(duration) => Ok(WithSpan::Duration(*duration)),
		Value::Uint8(count) => Ok(WithSpan::Count(*count)),
		_ => Err(internal_error!("unexpected apply with entry {}: {:?}", key, value)),
	}
}

fn duration_span(key: &'static str, span: Option<WithSpan>) -> Result<Option<Duration>, ValueError> {
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

	use super::{
		ApplyWith, JoinSpans, JoinWith, LookupWith, OnEmpty, WindowKind, WindowKnobs, WindowSize, WithSpan,
		decode_apply_with, encode_apply_with,
	};

	fn secs(n: i64) -> Duration {
		Duration::from_seconds(n).unwrap()
	}

	fn with(drop_span: Option<i64>, immutable: Option<i64>) -> ApplyWith {
		ApplyWith {
			window: None,
			drop: drop_span.map(|n| WithSpan::Duration(secs(n))),
			immutable: immutable.map(|n| WithSpan::Duration(secs(n))),
			forget: None,
		}
	}

	#[test]
	fn declared_immutable_below_drop_is_kept_verbatim() {
		assert_eq!(
			WindowKnobs::from_operator_with(&with(Some(20), Some(15))).unwrap(),
			WindowKnobs {
				drop: Some(secs(20)),
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn drop_without_immutable_resolves_with_no_immutable() {
		// Substituting the drop would arm the immutable slots on a window that never asked to fold.
		assert_eq!(
			WindowKnobs::from_operator_with(&with(Some(20), None)).unwrap(),
			WindowKnobs {
				drop: Some(secs(20)),
				immutable: None,
			}
		);
	}

	#[test]
	fn declared_immutable_equal_to_drop_is_rejected() {
		// The bound is strict; an immutable equal to the drop would never fold before the window closes.
		assert!(WindowKnobs::from_operator_with(&with(Some(20), Some(20))).is_err());
	}

	#[test]
	fn declared_immutable_above_drop_is_rejected() {
		assert!(WindowKnobs::from_operator_with(&with(Some(20), Some(30))).is_err());
	}

	#[test]
	fn immutable_without_drop_is_accepted() {
		// The ordering bound needs both knobs, so an immutable alone must reach the operator untouched.
		assert_eq!(
			WindowKnobs::from_operator_with(&with(None, Some(15))).unwrap(),
			WindowKnobs {
				drop: None,
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn neither_knob_declared_leaves_both_absent() {
		// An undeclared knob must stay absent, never resolve to zero, which is itself a legal declared value.
		assert_eq!(
			WindowKnobs::from_operator_with(&with(None, None)).unwrap(),
			WindowKnobs {
				drop: None,
				immutable: None,
			}
		);
	}

	#[test]
	fn validated_returns_both_when_declared() {
		assert_eq!(
			WindowKnobs::from_operator_with(&with(Some(20), Some(15))).unwrap(),
			WindowKnobs {
				drop: Some(secs(20)),
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn validated_returns_no_immutable_when_only_the_drop_is_declared() {
		// A window under the immutable floor declares a drop alone and must still resolve.
		assert_eq!(
			WindowKnobs::from_operator_with(&with(Some(20), None)).unwrap(),
			WindowKnobs {
				drop: Some(secs(20)),
				immutable: None,
			}
		);
	}

	#[test]
	fn validated_names_the_ordering_violation() {
		let err = WindowKnobs::from_operator_with(&with(Some(20), Some(20))).unwrap_err();
		assert!(err.to_string().contains("must be strictly less than drop"), "{err}");
	}

	#[test]
	fn validated_accepts_an_immutable_without_a_drop() {
		// Validation covers the ordering rule only; neither knob is required, so this must not fail.
		assert_eq!(
			WindowKnobs::from_operator_with(&with(None, Some(15))).unwrap(),
			WindowKnobs {
				drop: None,
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn validated_accepts_a_window_that_declares_no_knob_at_all() {
		assert_eq!(
			WindowKnobs::from_operator_with(&with(None, None)).unwrap(),
			WindowKnobs {
				drop: None,
				immutable: None,
			}
		);
	}

	#[test]
	fn a_count_span_is_rejected_for_a_time_sealed_operator() {
		// A count read as a duration would seal after that many nanoseconds instead of that many rows.
		let with = ApplyWith {
			window: None,
			drop: Some(WithSpan::Count(150)),
			immutable: None,
			forget: None,
		};
		let err = WindowKnobs::from_operator_with(&with).unwrap_err();
		assert!(err.to_string().contains("drop"), "{err}");
	}

	#[test]
	fn apply_with_round_trips_through_the_create_buffer() {
		// A span that changes unit on the way to the guest seals after rows instead of time, or the reverse.
		for with in [
			ApplyWith::default(),
			with(Some(30), Some(10)),
			ApplyWith {
				window: None,
				drop: Some(WithSpan::Count(150)),
				immutable: Some(WithSpan::Count(0)),
				forget: None,
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
		let values = HashMap::from([("drop".to_string(), Value::Int8(5))]);
		let bytes = encode_params(&Params::Named(Arc::new(values))).unwrap();
		assert!(decode_apply_with(&bytes).is_err());
	}

	#[test]
	fn ordinal_params_fail_to_decode() {
		let bytes = encode_params(&Params::Ordinal(Arc::new(vec![Value::Duration(secs(1))]))).unwrap();
		assert!(decode_apply_with(&bytes).is_err());
	}

	#[test]
	fn every_window_kind_round_trips_through_the_create_buffer() {
		// A kind that decodes into the wrong shape would run the guest with a size or slide in the wrong unit.
		let drop_span = Some(WithSpan::Duration(secs(30)));
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
				on_empty: OnEmpty::Remove,
			},
			WindowKind::Rolling {
				size: WindowSize::Duration(secs(60)),
				lag: None,
				pane: None,
				on_empty: OnEmpty::Remove,
			},
			WindowKind::Session {
				gap: secs(30),
			},
		];
		for kind in kinds {
			let with = ApplyWith {
				window: Some(kind),
				drop: drop_span,
				immutable: None,
				forget: None,
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
			drop: None,
			immutable: None,
			forget: None,
		};
		assert!(count.window_duration().is_err());

		let session = ApplyWith {
			window: Some(WindowKind::Session {
				gap: secs(30),
			}),
			drop: None,
			immutable: None,
			forget: None,
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
			drop: None,
			immutable: None,
			forget: None,
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
			drop: None,
			immutable: None,
			forget: None,
		};
		let by_slots = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Count(10),
				slide: WindowSize::Count(4),
			}),
			drop: None,
			immutable: None,
			forget: None,
		};
		let tumbling = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			drop: None,
			immutable: None,
			forget: None,
		};

		assert_eq!(by_time.window_slide_duration().unwrap(), Some(secs(15)));
		assert_eq!(by_slots.window_slide_slots().unwrap(), Some(4));
		assert!(by_time.window_slide_slots().is_err());
		assert!(by_slots.window_slide_duration().is_err());
		assert_eq!(tumbling.window_slide_duration().unwrap(), None);
		assert_eq!(tumbling.window_slide_slots().unwrap(), None);
	}

	#[test]
	fn window_session_gap_reads_only_a_session_window() {
		// A gap read from another kind would give a tumbling or sliding window a session's sealing with no
		// error.
		let session = ApplyWith {
			window: Some(WindowKind::Session {
				gap: secs(30),
			}),
			drop: None,
			immutable: None,
			forget: None,
		};
		let others = [
			WindowKind::Tumbling {
				size: WindowSize::Duration(secs(30)),
			},
			WindowKind::Sliding {
				size: WindowSize::Duration(secs(30)),
				slide: WindowSize::Duration(secs(10)),
			},
			WindowKind::Rolling {
				size: WindowSize::Duration(secs(30)),
				lag: None,
				pane: None,
				on_empty: OnEmpty::Remove,
			},
		];

		assert_eq!(session.window_session_gap(), Some(secs(30)));
		assert_eq!(ApplyWith::default().window_session_gap(), None);
		for kind in others {
			let name = kind.name();
			let with = ApplyWith {
				window: Some(kind),
				drop: None,
				immutable: None,
				forget: None,
			};
			assert_eq!(with.window_session_gap(), None, "a {name} window has no session gap");
		}
	}

	#[test]
	fn check_session_window_refuses_a_positive_immutable_span_only_on_a_session() {
		// An immutable row cannot follow its session through a split or merge, so it would count twice.
		let with = |window: WindowKind, immutable: Option<WithSpan>| ApplyWith {
			window: Some(window),
			drop: None,
			immutable,
			forget: None,
		};
		let session = WindowKind::Session {
			gap: secs(30),
		};
		let tumbling = WindowKind::Tumbling {
			size: WindowSize::Duration(secs(60)),
		};

		assert!(with(session.clone(), None).check_session_window().is_ok());
		assert!(with(session.clone(), Some(WithSpan::Duration(secs(0)))).check_session_window().is_ok());
		assert_eq!(
			with(session, Some(WithSpan::Duration(secs(1)))).check_session_window().unwrap_err().0.code,
			"FLOW_093"
		);
		assert!(with(tumbling, Some(WithSpan::Duration(secs(1)))).check_session_window().is_ok());
	}

	#[test]
	fn check_session_window_refuses_a_zero_gap_and_a_count_drop_only_on_a_session() {
		// A zero gap spans no row and a count drop is the wrong unit; refusing a duration drop would
		// split the sdk from native.
		let session = |gap: Duration, drop_span: Option<WithSpan>| ApplyWith {
			window: Some(WindowKind::Session {
				gap,
			}),
			drop: drop_span,
			immutable: None,
			forget: None,
		};
		let code = |with: ApplyWith| with.check_session_window().unwrap_err().0.code;

		assert!(session(secs(30), None).check_session_window().is_ok());
		assert!(session(secs(30), Some(WithSpan::Duration(secs(0)))).check_session_window().is_ok());
		assert!(session(secs(30), Some(WithSpan::Duration(secs(1)))).check_session_window().is_ok());
		assert_eq!(code(session(secs(0), None)), "FLOW_079");
		assert_eq!(
			code(session(secs(0), Some(WithSpan::Duration(secs(1))))),
			"FLOW_079",
			"the gap is checked first"
		);
		assert_eq!(
			code(session(secs(30), Some(WithSpan::Count(5)))),
			"FLOW_063",
			"a session seals by time, so a count drop is the wrong unit"
		);

		let tumbling = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			drop: Some(WithSpan::Duration(secs(1))),
			immutable: None,
			forget: None,
		};
		assert!(tumbling.check_session_window().is_ok());
		assert!(ApplyWith::default().check_session_window().is_ok());
	}

	#[test]
	fn reject_window_refuses_any_window() {
		assert!(ApplyWith::default().reject_window().is_ok());

		let windowed = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			drop: None,
			immutable: None,
			forget: None,
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
				on_empty: OnEmpty::Remove,
			}),
			drop: None,
			immutable: None,
			forget: None,
		};
		assert!(rolling.require_window("tumbling").is_err());
		assert!(rolling.require_window("rolling").is_ok());
	}

	#[test]
	fn drop_readers_refuse_the_other_unit() {
		let none = ApplyWith::default();
		assert_eq!(none.drop_duration().unwrap(), None);
		assert_eq!(none.drop_count().unwrap(), None);

		let duration = ApplyWith {
			window: None,
			drop: Some(WithSpan::Duration(secs(30))),
			immutable: None,
			forget: None,
		};
		assert_eq!(duration.drop_duration().unwrap(), Some(secs(30)));
		assert!(duration.drop_count().is_err());

		let count = ApplyWith {
			window: None,
			drop: Some(WithSpan::Count(5)),
			immutable: None,
			forget: None,
		};
		assert_eq!(count.drop_count().unwrap(), Some(5));
		assert!(count.drop_duration().is_err());
	}

	#[test]
	fn a_rolling_pane_round_trips_through_the_create_buffer() {
		// A pane lost or moved between the reader and the guest would run the window at the wrong resolution.
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(secs(3600)),
				lag: Some(secs(5)),
				pane: Some(secs(1)),
				on_empty: OnEmpty::Remove,
			}),
			drop: None,
			immutable: None,
			forget: None,
		};
		assert_eq!(decode_apply_with(&encode_apply_with(&with).unwrap()).unwrap(), with);
	}

	#[test]
	fn a_rolling_carry_round_trips_through_the_create_buffer_and_zero_never_encodes() {
		// a carry lost on the way to the guest removes rows, and an encoded zero would reach a guest that has
		// none.
		let with = |on_empty| ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(secs(60)),
				lag: None,
				pane: Some(secs(1)),
				on_empty,
			}),
			drop: None,
			immutable: None,
			forget: None,
		};
		for on_empty in [OnEmpty::Remove, OnEmpty::Carry] {
			assert_eq!(
				decode_apply_with(&encode_apply_with(&with(on_empty)).unwrap()).unwrap(),
				with(on_empty)
			);
		}
		assert!(encode_apply_with(&with(OnEmpty::Zero)).is_err(), "a zero must never reach a guest");
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
			drop: None,
			immutable: Some(WithSpan::Count(4)),
			forget: None,
		};
		assert_eq!(count.immutable_count().unwrap(), Some(4));

		let duration = ApplyWith {
			window: None,
			drop: None,
			immutable: Some(WithSpan::Duration(secs(4))),
			forget: None,
		};
		assert!(duration.immutable_count().is_err());
	}

	#[test]
	fn forget_round_trips_through_the_create_buffer() {
		// A forget dropped on the way to the guest would run the view with a bound it did not declare.
		let declared = ApplyWith {
			forget: Some(secs(3600)),
			..with(Some(20), None)
		};
		let bytes = encode_apply_with(&declared).unwrap();
		assert_eq!(decode_apply_with(&bytes).unwrap(), declared);
	}

	#[test]
	fn effective_drop_is_the_drop_or_else_the_forget() {
		// A forget alone must cut arrivals at its own line, and a drop alone must never free state.
		let forget_alone = ApplyWith {
			forget: Some(secs(60)),
			..with(None, None)
		};
		assert_eq!(forget_alone.effective_drop().unwrap(), Some(secs(60)));

		let drop_alone = with(Some(20), None);
		assert_eq!(drop_alone.effective_drop().unwrap(), Some(secs(20)));
		assert_eq!(drop_alone.forget, None);

		assert_eq!(with(None, None).effective_drop().unwrap(), None);

		let both = ApplyWith {
			forget: Some(secs(60)),
			..with(Some(20), None)
		};
		assert_eq!(both.effective_drop().unwrap(), Some(secs(20)));
		assert_eq!(both.forget, Some(secs(60)));
	}

	fn spans(left: Option<i64>, right: Option<i64>) -> Option<JoinSpans> {
		Some(JoinSpans {
			left: left.map(secs),
			right: right.map(secs),
		})
	}

	#[test]
	fn a_join_forget_below_the_drop_of_its_side_is_refused_and_equal_is_accepted() {
		// A forget under the drop of its side frees stored rows while changes for them are still accepted.
		let join = |drop_span, forget| JoinWith {
			drop: drop_span,
			forget,
			..JoinWith::default()
		};

		let left_below = join(spans(Some(20), None), spans(Some(19), None)).check_forget().unwrap_err();
		assert_eq!(left_below.0.code, "FLOW_081");
		assert!(left_below.to_string().contains("must not be below drop"), "{left_below}");

		let right_below = join(spans(Some(5), Some(20)), spans(Some(60), Some(19))).check_forget().unwrap_err();
		assert_eq!(right_below.0.code, "FLOW_081");

		assert!(join(spans(Some(20), Some(30)), spans(Some(20), Some(30))).check_forget().is_ok());
		assert!(join(spans(Some(20), None), spans(None, Some(1))).check_forget().is_ok());
		assert!(join(spans(Some(20), Some(20)), None).check_forget().is_ok());
		assert!(join(None, spans(Some(1), Some(1))).check_forget().is_ok());
	}

	#[test]
	fn a_lookup_forget_below_its_drop_is_refused_and_equal_is_accepted() {
		// A forget under the drop frees stored left rows while changes for them are still accepted.
		let lookup = |drop_span: Option<i64>, forget: Option<i64>| LookupWith {
			drop: drop_span.map(secs),
			forget: forget.map(secs),
			..LookupWith::default()
		};

		let below = lookup(Some(20), Some(19)).check_forget().unwrap_err();
		assert_eq!(below.0.code, "FLOW_081");
		assert!(below.to_string().contains("must not be below drop"), "{below}");

		assert!(lookup(Some(20), Some(20)).check_forget().is_ok());
		assert!(lookup(Some(20), None).check_forget().is_ok());
		assert!(lookup(None, Some(1)).check_forget().is_ok());
	}

	#[test]
	fn the_effective_drop_of_a_join_side_falls_back_to_the_forget_of_that_side_only() {
		// A side cut by the other side's forget would refuse arrivals its own with block never bounded.
		let forget_left_only = JoinWith {
			forget: spans(Some(60), None),
			..JoinWith::default()
		};
		assert_eq!(forget_left_only.effective_drop_left(), Some(secs(60)));
		assert_eq!(forget_left_only.effective_drop_right(), None);
		assert_eq!(forget_left_only.forget_left(), Some(secs(60)));
		assert_eq!(forget_left_only.forget_right(), None);

		let mixed = JoinWith {
			drop: spans(Some(20), None),
			forget: spans(Some(60), Some(90)),
			..JoinWith::default()
		};
		assert_eq!(mixed.effective_drop_left(), Some(secs(20)));
		assert_eq!(mixed.effective_drop_right(), Some(secs(90)));

		let drop_only = JoinWith {
			drop: spans(None, Some(20)),
			..JoinWith::default()
		};
		assert_eq!(drop_only.effective_drop_left(), None);
		assert_eq!(drop_only.effective_drop_right(), Some(secs(20)));
		assert_eq!(drop_only.forget_right(), None);

		let lookup = LookupWith {
			forget: Some(secs(60)),
			..LookupWith::default()
		};
		assert_eq!(lookup.effective_drop(), Some(secs(60)));
	}

	#[test]
	fn check_forget_refuses_below_the_drop_and_accepts_equal() {
		// A forget under the drop frees a group while a timer inside the hold can still fire for it.
		let below = ApplyWith {
			forget: Some(secs(19)),
			..with(Some(20), None)
		};
		let err = below.check_forget().unwrap_err();
		assert!(err.to_string().contains("must not be below drop"), "{err}");

		let equal = ApplyWith {
			forget: Some(secs(20)),
			..with(Some(20), None)
		};
		assert!(equal.check_forget().is_ok());
		assert!(with(Some(20), None).check_forget().is_ok());
	}

	#[test]
	fn reject_forget_refuses_only_a_declared_forget() {
		// A forget silently dropped on a class that never reclaims by it is a bound the author believes
		// holds.
		assert!(with(Some(20), None).reject_forget().is_ok());
		let declared = ApplyWith {
			forget: Some(secs(60)),
			..with(None, None)
		};
		let err = declared.reject_forget().unwrap_err();
		assert!(err.to_string().contains("takes no 'forget'"), "{err}");
	}
}
