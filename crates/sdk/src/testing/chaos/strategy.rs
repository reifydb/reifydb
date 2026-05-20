// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Range, sync::Arc};

use rand::{RngExt, rngs::StdRng};
use reifydb_core::{encoded::shape::RowShape, row::Row};
use reifydb_type::value::{
	Value, date::Date, datetime::DateTime, duration::Duration, row_number::RowNumber, time::Time, r#type::Type,
};

use super::schema::{ChaosSchema, KeyStrategy};
use crate::testing::builders::TestRowBuilder;

pub type ColumnSampler = Arc<dyn Fn(&mut StdRng) -> Value + Send + Sync>;

pub type RowConstraint = Arc<dyn Fn(&mut RowContent) + Send + Sync>;

#[derive(Debug, Clone, Default)]
pub struct RowContent {
	fields: HashMap<String, Value>,
}

impl RowContent {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn from_pairs<I>(pairs: I) -> Self
	where
		I: IntoIterator<Item = (String, Value)>,
	{
		Self {
			fields: pairs.into_iter().collect(),
		}
	}

	pub fn get(&self, name: &str) -> Option<&Value> {
		self.fields.get(name)
	}

	pub fn set(&mut self, name: impl Into<String>, value: Value) {
		self.fields.insert(name.into(), value);
	}

	pub fn contains(&self, name: &str) -> bool {
		self.fields.contains_key(name)
	}

	pub fn f64(&self, name: &str) -> Option<f64> {
		match self.fields.get(name)? {
			Value::Float8(of) => Some(of.value()),
			Value::Float4(of) => Some(of.value() as f64),
			_ => None,
		}
	}

	pub fn u64(&self, name: &str) -> Option<u64> {
		match self.fields.get(name)? {
			Value::Uint8(v) => Some(*v),
			Value::Uint4(v) => Some(*v as u64),
			Value::Uint2(v) => Some(*v as u64),
			Value::Uint1(v) => Some(*v as u64),
			_ => None,
		}
	}

	pub fn i64(&self, name: &str) -> Option<i64> {
		match self.fields.get(name)? {
			Value::Int8(v) => Some(*v),
			Value::Int4(v) => Some(*v as i64),
			Value::Int2(v) => Some(*v as i64),
			Value::Int1(v) => Some(*v as i64),
			_ => None,
		}
	}

	pub fn utf8(&self, name: &str) -> Option<&str> {
		match self.fields.get(name)? {
			Value::Utf8(s) => Some(s.as_str()),
			_ => None,
		}
	}

	pub fn boolean(&self, name: &str) -> Option<bool> {
		match self.fields.get(name)? {
			Value::Boolean(b) => Some(*b),
			_ => None,
		}
	}
}

#[derive(Default, Clone)]
pub struct ColumnRegistry {
	samplers: HashMap<String, ColumnSampler>,
	pub(super) constraint: Option<RowConstraint>,
}

impl ColumnRegistry {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn register(&mut self, name: impl Into<String>, sampler: ColumnSampler) {
		self.samplers.insert(name.into(), sampler);
	}

	pub fn set_constraint(&mut self, constraint: RowConstraint) {
		self.constraint = Some(constraint);
	}

	pub fn has_sampler(&self, name: &str) -> bool {
		self.samplers.contains_key(name)
	}

	pub(crate) fn validate(&self, input_shape: &RowShape) -> Result<(), Vec<String>> {
		let missing: Vec<String> = input_shape
			.field_names()
			.filter(|name| !self.samplers.contains_key(*name))
			.map(|s| s.to_string())
			.collect();
		if missing.is_empty() {
			Ok(())
		} else {
			Err(missing)
		}
	}
}

pub fn sample_row(
	schema: &ChaosSchema,
	registry: &ColumnRegistry,
	rng: &mut StdRng,
	next_sequential: u64,
) -> (Row, RowContent) {
	let mut content = RowContent::new();

	for field in schema.input_shape.fields() {
		let value = match registry.samplers.get(&field.name) {
			Some(sampler) => sampler(rng),
			None => Value::none_of(field.constraint.get_type()),
		};
		content.set(field.name.clone(), value);
	}

	if let Some(constraint) = &registry.constraint {
		constraint(&mut content);
	}

	let row_number = match &schema.key_strategy {
		KeyStrategy::Sequential => RowNumber(next_sequential),
		KeyStrategy::HashOf(_) | KeyStrategy::Custom(_) => {
			schema.key_strategy.derive(&content, next_sequential)
		}
	};

	let row = encode_row(schema, &content, row_number);
	(row, content)
}

pub fn encode_row(schema: &ChaosSchema, content: &RowContent, row_number: RowNumber) -> Row {
	let values: Vec<Value> = schema
		.input_shape
		.fields()
		.iter()
		.map(|f| content.get(&f.name).cloned().unwrap_or_else(|| Value::none_of(f.constraint.get_type())))
		.collect();

	TestRowBuilder::new(row_number).with_values(values).with_shape(schema.input_shape.clone()).build()
}

pub mod samplers {
	use super::*;

	pub fn select<V: Into<Value> + Clone + Send + Sync + 'static>(values: &[V]) -> ColumnSampler {
		let owned: Vec<Value> = values.iter().cloned().map(Into::into).collect();
		assert!(!owned.is_empty(), "select() requires at least one value");
		Arc::new(move |rng| {
			let idx = rng.random_range(0..owned.len());
			owned[idx].clone()
		})
	}

	pub fn u64_range(range: Range<u64>) -> ColumnSampler {
		Arc::new(move |rng| Value::uint8(rng.random_range(range.clone())))
	}

	pub fn i64_range(range: Range<i64>) -> ColumnSampler {
		Arc::new(move |rng| Value::int8(rng.random_range(range.clone())))
	}

	pub fn u32_range(range: Range<u32>) -> ColumnSampler {
		Arc::new(move |rng| Value::uint4(rng.random_range(range.clone())))
	}

	pub fn datetime_range(range: Range<DateTime>) -> ColumnSampler {
		assert!(range.start < range.end, "datetime_range start must be < end");
		let start_nanos = range.start.to_nanos();
		let end_nanos = range.end.to_nanos();
		Arc::new(move |rng| {
			let nanos = rng.random_range(start_nanos..end_nanos);
			Value::datetime(DateTime::from_nanos(nanos))
		})
	}

	pub fn duration_range(range: Range<Duration>) -> ColumnSampler {
		let start_nanos = range.start.nanoseconds();
		let end_nanos = range.end.nanoseconds();
		assert!(start_nanos < end_nanos, "duration_range start must be < end");
		Arc::new(move |rng| {
			let nanos = rng.random_range(start_nanos..end_nanos);
			Value::duration(Duration::from_nanoseconds(nanos).expect("duration_range bounds must be valid"))
		})
	}

	pub fn date_range(range: Range<Date>) -> ColumnSampler {
		let start_days = range.start.to_days_since_epoch();
		let end_days = range.end.to_days_since_epoch();
		assert!(start_days < end_days, "date_range start must be < end");
		Arc::new(move |rng| {
			let days = rng.random_range(start_days..end_days);
			Value::date(Date::from_days_since_epoch(days).expect("date_range bounds must be valid"))
		})
	}

	pub fn time_range(range: Range<Time>) -> ColumnSampler {
		let start_nanos = range.start.to_nanos_since_midnight();
		let end_nanos = range.end.to_nanos_since_midnight();
		assert!(start_nanos < end_nanos, "time_range start must be < end");
		Arc::new(move |rng| {
			let nanos = rng.random_range(start_nanos..end_nanos);
			Value::time(Time::from_nanos_since_midnight(nanos).expect("time_range bounds must be valid"))
		})
	}

	pub fn f64_range(range: Range<f64>) -> ColumnSampler {
		assert!(range.start.is_finite() && range.end.is_finite(), "f64_range bounds must be finite");
		assert!(range.start < range.end, "f64_range start must be < end");
		let start = range.start;
		let span = range.end - range.start;
		Arc::new(move |rng| {
			let r: f64 = rng.random_range(0.0..1.0);
			Value::float8(start + r * span)
		})
	}

	pub fn utf8_choices(choices: &[&str]) -> ColumnSampler {
		assert!(!choices.is_empty(), "utf8_choices requires at least one string");
		let owned: Vec<String> = choices.iter().map(|s| (*s).to_string()).collect();
		Arc::new(move |rng| {
			let idx = rng.random_range(0..owned.len());
			Value::utf8(owned[idx].clone())
		})
	}

	pub fn constant(value: Value) -> ColumnSampler {
		Arc::new(move |_rng| value.clone())
	}

	pub fn always_none(field_type: Type) -> ColumnSampler {
		Arc::new(move |_rng| Value::none_of(field_type.clone()))
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use rand::SeedableRng;
	use reifydb_core::encoded::shape::RowShapeField;

	use super::*;

	fn shape(fields: &[(&str, Type)]) -> RowShape {
		RowShape::new(fields.iter().map(|(n, t)| RowShapeField::unconstrained(*n, t.clone())).collect())
	}

	fn schema_basic() -> ChaosSchema {
		ChaosSchema {
			input_shape: shape(&[
				("base", Type::Utf8),
				("quote", Type::Utf8),
				("slot", Type::Uint8),
				("price", Type::Float8),
			]),
			output_shape: shape(&[("base", Type::Utf8), ("quote", Type::Utf8), ("slot", Type::Uint8)]),
			key_strategy: KeyStrategy::hash_of(["base", "quote", "slot"]),
			output_key_columns: vec!["base".into(), "quote".into(), "slot".into()],
		}
	}

	fn registry_basic() -> ColumnRegistry {
		let mut reg = ColumnRegistry::new();
		reg.register("base", samplers::utf8_choices(&["A", "B"]));
		reg.register("quote", samplers::utf8_choices(&["SOL", "USDC"]));
		reg.register("slot", samplers::u64_range(1..100));
		reg.register("price", samplers::f64_range(1.0..10.0));
		reg
	}

	#[test]
	fn sample_row_is_reproducible_for_a_seed() {
		let schema = schema_basic();
		let registry = registry_basic();

		let mut rng_a = StdRng::seed_from_u64(42);
		let mut rng_b = StdRng::seed_from_u64(42);
		let (row_a, _) = sample_row(&schema, &registry, &mut rng_a, 1);
		let (row_b, _) = sample_row(&schema, &registry, &mut rng_b, 1);

		assert_eq!(row_a.number, row_b.number);
		assert_eq!(row_a.shape, row_b.shape);
		// EncodedRow doesn't impl PartialEq directly; compare bytes.
		assert_eq!(row_a.encoded.as_slice(), row_b.encoded.as_slice());
	}

	#[test]
	fn sample_row_different_seeds_diverge() {
		let schema = schema_basic();
		let registry = registry_basic();

		// Two different seeds should produce different rows. Run many
		// pairs to ensure we don't get unlucky on a single one.
		let mut diverged = 0;
		for s in 0..20u64 {
			let mut rng_a = StdRng::seed_from_u64(s);
			let mut rng_b = StdRng::seed_from_u64(s + 100);
			let (row_a, _) = sample_row(&schema, &registry, &mut rng_a, 1);
			let (row_b, _) = sample_row(&schema, &registry, &mut rng_b, 1);
			if row_a.encoded.as_slice() != row_b.encoded.as_slice() {
				diverged += 1;
			}
		}
		assert!(diverged > 15, "expected most seed pairs to produce different rows; got {diverged}/20");
	}

	#[test]
	fn hash_of_collides_on_same_key_columns() {
		// Two rows whose `base`/`quote`/`slot` happen to match must share
		// a RowNumber regardless of `price`.
		let schema = schema_basic();
		let mut reg = ColumnRegistry::new();
		reg.register("base", samplers::constant(Value::utf8("A")));
		reg.register("quote", samplers::constant(Value::utf8("SOL")));
		reg.register("slot", samplers::constant(Value::uint8(42u64)));
		// Price varies wildly; should not affect RowNumber.
		reg.register("price", samplers::f64_range(0.0..1000.0));

		let mut rng = StdRng::seed_from_u64(1);
		let (row_a, _) = sample_row(&schema, &reg, &mut rng, 1);
		let (row_b, _) = sample_row(&schema, &reg, &mut rng, 2);
		let (row_c, _) = sample_row(&schema, &reg, &mut rng, 3);

		assert_eq!(row_a.number, row_b.number, "constant key cols -> same RowNumber");
		assert_eq!(row_b.number, row_c.number);
	}

	#[test]
	fn row_constraint_overrides_sampled_values() {
		let schema = ChaosSchema {
			input_shape: shape(&[
				("base_volume", Type::Float8),
				("price", Type::Float8),
				("quote_volume", Type::Float8),
			]),
			output_shape: shape(&[("base_volume", Type::Float8)]),
			key_strategy: KeyStrategy::Sequential,
			output_key_columns: vec!["base_volume".into()],
		};
		let mut reg = ColumnRegistry::new();
		reg.register("base_volume", samplers::constant(Value::float8(2.0_f64)));
		reg.register("price", samplers::constant(Value::float8(3.0_f64)));
		// Pre-constraint sampled value will be overwritten by the
		// constraint closure.
		reg.register("quote_volume", samplers::constant(Value::float8(0.0_f64)));
		reg.set_constraint(Arc::new(|content| {
			let bv = content.f64("base_volume").unwrap();
			let p = content.f64("price").unwrap();
			content.set("quote_volume", Value::float8(bv * p));
		}));

		let mut rng = StdRng::seed_from_u64(0);
		let (row, content) = sample_row(&schema, &reg, &mut rng, 1);

		// Constraint result available in content directly.
		assert!((content.f64("quote_volume").unwrap() - 6.0).abs() < 1e-12);

		// Constraint result also reflected in encoded row. Field index = 2.
		let quote_volume_field = row.shape.find_field("quote_volume").expect("field");
		let buf = &row.encoded.as_slice()[quote_volume_field.offset as usize
			..(quote_volume_field.offset as usize + quote_volume_field.size as usize)];
		let mut bytes = [0u8; 8];
		bytes.copy_from_slice(buf);
		let v = f64::from_le_bytes(bytes);
		assert!((v - 6.0).abs() < 1e-12, "quote_volume should be 2.0 * 3.0 = 6.0, got {v}");
	}

	#[test]
	fn registry_validate_catches_missing_sampler() {
		let s = shape(&[("a", Type::Int8), ("b", Type::Int8)]);
		let mut reg = ColumnRegistry::new();
		reg.register("a", samplers::constant(Value::int8(0_i64)));
		// "b" intentionally omitted.
		let missing = reg.validate(&s).expect_err("should reject");
		assert_eq!(missing, vec!["b".to_string()]);
	}

	#[test]
	fn registry_validate_reports_all_missing_columns() {
		let s = shape(&[("a", Type::Int8), ("b", Type::Int8), ("c", Type::Int8)]);
		let reg = ColumnRegistry::new();
		// Nothing registered.
		let missing = reg.validate(&s).expect_err("should reject");
		// Order follows shape field order.
		assert_eq!(missing, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
	}

	#[test]
	fn registry_validate_accepts_full_coverage() {
		let s = shape(&[("a", Type::Int8)]);
		let mut reg = ColumnRegistry::new();
		reg.register("a", samplers::constant(Value::int8(0_i64)));
		assert!(reg.validate(&s).is_ok());
	}

	#[test]
	fn samplers_select_picks_from_set() {
		let s = samplers::select(&[Value::int8(1_i64), Value::int8(2_i64), Value::int8(3_i64)]);
		let mut rng = StdRng::seed_from_u64(0);
		let mut seen = HashSet::new();
		for _ in 0..100 {
			seen.insert(format!("{:?}", s(&mut rng)));
		}
		// With 100 samples from 3 values, all 3 should appear.
		assert_eq!(seen.len(), 3, "expected all 3 values eventually; saw {seen:?}");
	}

	#[test]
	fn samplers_f64_range_is_in_bounds() {
		let s = samplers::f64_range(5.0..10.0);
		let mut rng = StdRng::seed_from_u64(0);
		for _ in 0..1000 {
			let v = s(&mut rng);
			let f = match v {
				Value::Float8(of) => of.value(),
				_ => panic!("expected Float8"),
			};
			assert!((5.0..10.0).contains(&f), "out of range: {f}");
		}
	}

	#[test]
	fn samplers_datetime_range_is_in_bounds() {
		let s = samplers::datetime_range(
			DateTime::from_timestamp(1_000).unwrap()..DateTime::from_timestamp(2_000).unwrap(),
		);
		let mut rng = StdRng::seed_from_u64(0);
		for _ in 0..1000 {
			let secs = match s(&mut rng) {
				Value::DateTime(dt) => dt.timestamp(),
				other => panic!("expected DateTime, got {other:?}"),
			};
			assert!((1_000..2_000).contains(&secs), "out of range: {secs}");
		}
	}

	#[test]
	fn samplers_duration_range_is_in_bounds() {
		let s = samplers::duration_range(
			Duration::from_seconds(0).unwrap()..Duration::from_seconds(3_600).unwrap(),
		);
		let mut rng = StdRng::seed_from_u64(0);
		for _ in 0..1000 {
			let secs = match s(&mut rng) {
				Value::Duration(d) => d.seconds(),
				other => panic!("expected Duration, got {other:?}"),
			};
			assert!((0..3_600).contains(&secs), "out of range: {secs}");
		}
	}

	#[test]
	fn samplers_date_range_is_in_bounds() {
		let s = samplers::date_range(
			Date::from_days_since_epoch(0).unwrap()..Date::from_days_since_epoch(1_000).unwrap(),
		);
		let mut rng = StdRng::seed_from_u64(0);
		for _ in 0..1000 {
			let days = match s(&mut rng) {
				Value::Date(d) => d.to_days_since_epoch(),
				other => panic!("expected Date, got {other:?}"),
			};
			assert!((0..1_000).contains(&days), "out of range: {days}");
		}
	}

	#[test]
	fn samplers_time_range_is_in_bounds() {
		let range = 0..3_600_000_000_000u64;
		let time_range = Time::from_nanos_since_midnight(range.start).unwrap()
			..Time::from_nanos_since_midnight(range.end).unwrap();
		let s = samplers::time_range(time_range);
		let mut rng = StdRng::seed_from_u64(0);
		for _ in 0..1000 {
			let nanos = match s(&mut rng) {
				Value::Time(t) => t.to_nanos_since_midnight(),
				other => panic!("expected Time, got {other:?}"),
			};
			assert!(range.contains(&nanos), "out of range: {nanos}");
		}
	}

	#[test]
	fn row_content_typed_accessors() {
		let mut c = RowContent::new();
		c.set("f", Value::float8(1.5_f64));
		c.set("u", Value::uint8(42u64));
		c.set("i", Value::int8(-7_i64));
		c.set("s", Value::utf8("hello"));
		c.set("b", Value::Boolean(true));

		assert_eq!(c.f64("f"), Some(1.5));
		assert_eq!(c.u64("u"), Some(42));
		assert_eq!(c.i64("i"), Some(-7));
		assert_eq!(c.utf8("s"), Some("hello"));
		assert_eq!(c.boolean("b"), Some(true));

		// Wrong-type accesses return None instead of panicking.
		assert_eq!(c.f64("u"), None);
		assert_eq!(c.utf8("f"), None);
		assert_eq!(c.f64("missing"), None);
	}
}
