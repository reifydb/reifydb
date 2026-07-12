// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	Value, date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, int::Int, time::Time, uint::Uint,
};

use super::{
	Monoid, MonoidState,
	math::{count::Count, max::Max, min::Min, sum::Sum},
};

fn int1() -> Vec<Value> {
	vec![Value::Int1(1), Value::Int1(-2), Value::Int1(3), Value::Int1(0)]
}
fn int2() -> Vec<Value> {
	vec![Value::Int2(10), Value::Int2(-20), Value::Int2(30), Value::Int2(0)]
}
fn int4() -> Vec<Value> {
	vec![Value::Int4(100), Value::Int4(-200), Value::Int4(300), Value::Int4(0)]
}
fn int8() -> Vec<Value> {
	vec![Value::Int8(1_000), Value::Int8(-2_000), Value::Int8(3_000), Value::Int8(0)]
}
fn int16() -> Vec<Value> {
	vec![Value::Int16(10_000), Value::Int16(-20_000), Value::Int16(30_000), Value::Int16(0)]
}
fn uint1() -> Vec<Value> {
	vec![Value::Uint1(1), Value::Uint1(2), Value::Uint1(3), Value::Uint1(0)]
}
fn uint2() -> Vec<Value> {
	vec![Value::Uint2(10), Value::Uint2(20), Value::Uint2(30), Value::Uint2(0)]
}
fn uint4() -> Vec<Value> {
	vec![Value::Uint4(100), Value::Uint4(200), Value::Uint4(300), Value::Uint4(0)]
}
fn uint8() -> Vec<Value> {
	vec![Value::Uint8(1_000), Value::Uint8(2_000), Value::Uint8(3_000), Value::Uint8(0)]
}
fn uint16() -> Vec<Value> {
	vec![Value::Uint16(10_000), Value::Uint16(20_000), Value::Uint16(30_000), Value::Uint16(0)]
}
fn float4() -> Vec<Value> {
	vec![Value::float4(1.5f32), Value::float4(-2.5f32), Value::float4(3.25f32), Value::float4(0.0f32)]
}
fn float8() -> Vec<Value> {
	vec![Value::float8(1.5), Value::float8(-2.5), Value::float8(0.1), Value::float8(0.2), Value::float8(1e10)]
}
fn int_big() -> Vec<Value> {
	vec![
		Value::Int(Int::from_i64(123_456_789)),
		Value::Int(Int::from_i64(-987_654_321)),
		Value::Int(Int::from_i64(42)),
		Value::Int(Int::from_i64(0)),
	]
}
fn uint_big() -> Vec<Value> {
	vec![
		Value::Uint(Uint::from(1_000_000u64)),
		Value::Uint(Uint::from(2_000_000u64)),
		Value::Uint(Uint::from(3u64)),
		Value::Uint(Uint::from(0u64)),
	]
}
fn decimal() -> Vec<Value> {
	vec![
		Value::Decimal(Decimal::from(100i64)),
		Value::Decimal(Decimal::from(-50i64)),
		Value::Decimal(Decimal::from(25i64)),
		Value::Decimal(Decimal::from(0i64)),
	]
}

fn date() -> Vec<Value> {
	vec![
		Value::date(Date::new(2024, 1, 1).unwrap()),
		Value::date(Date::new(2023, 6, 15).unwrap()),
		Value::date(Date::new(2025, 12, 31).unwrap()),
	]
}
fn datetime() -> Vec<Value> {
	vec![
		Value::datetime(DateTime::new(2024, 1, 1, 0, 0, 0, 0).unwrap()),
		Value::datetime(DateTime::new(2023, 6, 15, 12, 30, 0, 0).unwrap()),
		Value::datetime(DateTime::new(2025, 12, 31, 23, 59, 59, 0).unwrap()),
	]
}
fn time() -> Vec<Value> {
	vec![
		Value::time(Time::new(1, 0, 0, 0).unwrap()),
		Value::time(Time::new(12, 30, 0, 0).unwrap()),
		Value::time(Time::new(23, 59, 59, 0).unwrap()),
	]
}
fn duration() -> Vec<Value> {
	vec![
		Value::duration(Duration::from_seconds(1).unwrap()),
		Value::duration(Duration::from_seconds(3_600).unwrap()),
		Value::duration(Duration::from_seconds(60).unwrap()),
	]
}

fn numeric_fixtures() -> Vec<(&'static str, Vec<Value>)> {
	vec![
		("int1", int1()),
		("int2", int2()),
		("int4", int4()),
		("int8", int8()),
		("int16", int16()),
		("uint1", uint1()),
		("uint2", uint2()),
		("uint4", uint4()),
		("uint8", uint8()),
		("uint16", uint16()),
		("float4", float4()),
		("int", int_big()),
		("uint", uint_big()),
		("decimal", decimal()),
	]
}

fn temporal_fixtures() -> Vec<(&'static str, Vec<Value>)> {
	vec![("date", date()), ("datetime", datetime()), ("time", time()), ("duration", duration())]
}

fn assert_states_eq(a: &MonoidState, b: &MonoidState, float_tolerant: bool) {
	assert_eq!(a.count, b.count, "count mismatch: {:?} vs {:?}", a, b);
	match (&a.value, &b.value) {
		(Value::Float8(x), Value::Float8(y)) if float_tolerant => {
			let scale = x.value().abs().max(y.value().abs()).max(1.0);
			let diff = (x.value() - y.value()).abs();
			assert!(
				diff <= scale * 1e-9,
				"float8 drift too large: {} vs {} ({:?} vs {:?})",
				x.value(),
				y.value(),
				a,
				b
			);
		}
		_ => assert_eq!(a.value, b.value, "value mismatch: {:?} vs {:?}", a, b),
	}
}

fn balanced_fold(m: &dyn Monoid, states: &[MonoidState]) -> MonoidState {
	match states.len() {
		0 => MonoidState::identity(),
		1 => states[0].clone(),
		n => {
			let mid = n / 2;
			let left = balanced_fold(m, &states[..mid]);
			let right = balanced_fold(m, &states[mid..]);
			m.combine(&left, &right).expect("combine should not fail for fixture values")
		}
	}
}

fn assert_monoid_laws(m: &dyn Monoid, values: &[Value], float_tolerant: bool) {
	assert!(values.len() >= 3, "fixture needs at least 3 values");

	let identity = MonoidState::identity();
	for v in values {
		let s = m.lift(v);
		let left = m.combine(&identity, &s).unwrap();
		let right = m.combine(&s, &identity).unwrap();
		assert_states_eq(&s, &left, float_tolerant);
		assert_states_eq(&s, &right, float_tolerant);
	}

	for a in values {
		for b in values {
			let (sa, sb) = (m.lift(a), m.lift(b));
			let ab = m.combine(&sa, &sb).unwrap();
			let ba = m.combine(&sb, &sa).unwrap();
			assert_states_eq(&ab, &ba, float_tolerant);
		}
	}

	for a in values {
		for b in values {
			for c in values {
				let (sa, sb, sc) = (m.lift(a), m.lift(b), m.lift(c));
				let left = m.combine(&m.combine(&sa, &sb).unwrap(), &sc).unwrap();
				let right = m.combine(&sa, &m.combine(&sb, &sc).unwrap()).unwrap();
				assert_states_eq(&left, &right, float_tolerant);
			}
		}
	}

	let states: Vec<MonoidState> = values.iter().map(|v| m.lift(v)).collect();
	let left_fold = states.iter().skip(1).fold(states[0].clone(), |acc, s| m.combine(&acc, s).unwrap());
	let right_fold =
		states.iter().rev().skip(1).fold(states.last().unwrap().clone(), |acc, s| m.combine(s, &acc).unwrap());
	let balanced = balanced_fold(m, &states);
	assert_states_eq(&left_fold, &right_fold, float_tolerant);
	assert_states_eq(&left_fold, &balanced, float_tolerant);
}

#[test]
fn sum_satisfies_monoid_laws_over_every_numeric_type() {
	let sum = Sum::new();
	for (name, values) in numeric_fixtures() {
		assert_monoid_laws(&sum, &values, false);
		let _ = name;
	}
	assert_monoid_laws(&sum, &float8(), true);
}

#[test]
fn min_satisfies_monoid_laws_over_every_orderable_type() {
	let min = Min::new();
	for (_, values) in numeric_fixtures().into_iter().chain(temporal_fixtures()) {
		assert_monoid_laws(&min, &values, false);
	}
}

#[test]
fn max_satisfies_monoid_laws_over_every_orderable_type() {
	let max = Max::new();
	for (_, values) in numeric_fixtures().into_iter().chain(temporal_fixtures()) {
		assert_monoid_laws(&max, &values, false);
	}
}

#[test]
fn count_satisfies_monoid_laws_over_every_type() {
	let count = Count::new();
	for (_, values) in numeric_fixtures().into_iter().chain(temporal_fixtures()) {
		assert_monoid_laws(&count, &values, false);
	}
	assert_monoid_laws(&count, &[Value::Utf8("a".into()), Value::Utf8("b".into()), Value::Boolean(true)], false);
}

#[test]
fn sum_invert_roundtrips_for_every_numeric_type() {
	let sum = Sum::new();
	for (name, values) in numeric_fixtures() {
		let s = sum.combine(&sum.lift(&values[0]), &sum.lift(&values[1])).unwrap();
		let extra = sum.lift(&values[2]);
		let total = sum.combine(&s, &extra).unwrap();
		let restored = sum.invert(&total, &extra).expect(name);
		assert_states_eq(&restored, &s, false);
	}
}

#[test]
fn sum_float8_invert_roundtrips_within_tolerance() {
	let sum = Sum::new();
	let values = float8();
	let s = sum.combine(&sum.lift(&values[0]), &sum.lift(&values[1])).unwrap();
	let extra = sum.lift(&values[2]);
	let total = sum.combine(&s, &extra).unwrap();
	let restored = sum.invert(&total, &extra).unwrap();
	assert_states_eq(&restored, &s, true);
}

#[test]
fn sum_invert_to_identity_at_count_zero() {
	let sum = Sum::new();
	let s = sum.lift(&Value::Int4(42));
	let restored = sum.invert(&s, &s).unwrap();
	assert_eq!(restored, MonoidState::identity());
}

#[test]
fn count_invert_roundtrips_and_is_always_some() {
	let count = Count::new();
	let a = count.lift(&Value::Int4(1));
	let b = count.lift(&Value::Int4(2));
	let total = count.combine(&a, &b).unwrap();
	let restored = count.invert(&total, &b).unwrap();
	assert_eq!(restored, a);
	assert_eq!(count.invert(&total, &MonoidState::identity()), Some(total.clone()));
}

#[test]
fn min_invert_returns_none_iff_removed_value_is_the_extreme() {
	let min = Min::new();
	let low = min.lift(&Value::Int4(1));
	let high = min.lift(&Value::Int4(5));
	let total = min.combine(&low, &high).unwrap();

	let restored = min.invert(&total, &high).unwrap();
	assert_states_eq(&restored, &low, false);

	assert_eq!(min.invert(&total, &low), None);
}

#[test]
fn max_invert_returns_none_iff_removed_value_is_the_extreme() {
	let max = Max::new();
	let low = max.lift(&Value::Int4(1));
	let high = max.lift(&Value::Int4(5));
	let total = max.combine(&low, &high).unwrap();

	let restored = max.invert(&total, &low).unwrap();
	assert_states_eq(&restored, &high, false);

	assert_eq!(max.invert(&total, &high), None);
}

#[test]
fn sum_overflow_errors_instead_of_saturating() {
	let sum = Sum::new();
	let at_ceiling = MonoidState {
		value: Value::Int16(i128::MAX),
		count: 1,
		compensation: 0.0,
	};
	let one = sum.lift(&Value::Int16(1));
	let err = sum.combine(&at_ceiling, &one);
	assert!(err.is_err(), "expected overflow to error, got {:?}", err);
}

#[test]
fn sum_float_non_finite_result_errors() {
	let sum = Sum::new();
	let huge = sum.lift(&Value::float8(f64::MAX));
	let err = sum.combine(&huge, &huge);
	assert!(err.is_err(), "expected non-finite float result to error, got {:?}", err);
}

#[test]
fn finalize_identity_is_none_for_sum_min_max_and_zero_for_count() {
	let identity = MonoidState::identity();
	assert!(matches!(Sum::new().finalize(&identity), Value::None { .. }));
	assert!(matches!(Min::new().finalize(&identity), Value::None { .. }));
	assert!(matches!(Max::new().finalize(&identity), Value::None { .. }));
	assert_eq!(Count::new().finalize(&identity), Value::Uint8(0));
}

#[test]
fn count_lift_ignores_the_actual_value() {
	let count = Count::new();
	assert_eq!(count.lift(&Value::Int4(999)), count.lift(&Value::Boolean(false)));
	assert_eq!(count.lift(&Value::Int4(999)).count, 1);
}

fn all_representative_states() -> Vec<MonoidState> {
	let mut states = vec![MonoidState::identity()];
	for (_, values) in numeric_fixtures().into_iter().chain(temporal_fixtures()) {
		for v in values {
			states.push(MonoidState {
				value: v,
				count: 1,
				compensation: 0.0,
			});
		}
	}
	states.push(MonoidState {
		value: Value::float8(1.5),
		count: 3,
		compensation: 1e-12,
	});
	states
}

#[test]
fn monoid_state_postcard_roundtrips_for_every_value_type() {
	for state in all_representative_states() {
		let bytes = postcard::to_stdvec(&state).unwrap();
		let decoded: MonoidState = postcard::from_bytes(&bytes).unwrap();
		assert_eq!(state, decoded);
	}
}

#[test]
fn monoid_state_vec_postcard_roundtrips() {
	let states = all_representative_states();
	let bytes = postcard::to_stdvec(&states).unwrap();
	let decoded: Vec<MonoidState> = postcard::from_bytes(&bytes).unwrap();
	assert_eq!(states, decoded);
}

#[test]
fn accepted_types_reject_non_numeric_for_sum() {
	let sum = Sum::new();
	assert!(!sum.accepted_types().accepts(0, &reifydb_value::value::value_type::ValueType::Utf8));
	assert!(sum.accepted_types().accepts(0, &reifydb_value::value::value_type::ValueType::Int4));
}

#[test]
fn accepted_types_for_min_max_include_temporal_but_not_utf8() {
	use reifydb_value::value::value_type::ValueType;
	for m in [Box::new(Min::new()) as Box<dyn Monoid>, Box::new(Max::new()) as Box<dyn Monoid>] {
		assert!(m.accepted_types().accepts(0, &ValueType::Date));
		assert!(m.accepted_types().accepts(0, &ValueType::Int4));
		assert!(!m.accepted_types().accepts(0, &ValueType::Utf8));
	}
}

#[test]
fn count_accepts_any_type() {
	let count = Count::new();
	assert!(count.accepted_types().accepts(0, &reifydb_value::value::value_type::ValueType::Utf8));
	assert!(count.accepted_types().accepts(0, &reifydb_value::value::value_type::ValueType::Boolean));
}

#[test]
fn state_type_widens_to_the_family_ceiling_for_sum_but_not_min_max_or_count() {
	use reifydb_value::value::value_type::ValueType;

	assert_eq!(Sum::new().state_type(ValueType::Int1), ValueType::Int16);
	assert_eq!(Sum::new().state_type(ValueType::Uint4), ValueType::Uint16);
	assert_eq!(Sum::new().state_type(ValueType::Float4), ValueType::Float8);
	assert_eq!(Sum::new().state_type(ValueType::Decimal), ValueType::Decimal);

	assert_eq!(Min::new().state_type(ValueType::Int4), ValueType::Int4);
	assert_eq!(Max::new().state_type(ValueType::Date), ValueType::Date);
	assert_eq!(Count::new().state_type(ValueType::Int4), ValueType::Uint8);
}
