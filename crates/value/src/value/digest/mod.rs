// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use libm::{ceil, log, pow};

use crate::{
	error::{Error, TypeError},
	value::{Value, duration::Duration, value_type::ValueType},
};

mod encoding;
pub mod literal;

const MIN_ACCURACY_PPM: u32 = 1_000;
const MAX_ACCURACY_PPM: u32 = 100_000;

const INNER_TAGS: [(u8, ValueType); 15] = [
	(2, ValueType::Float4),
	(3, ValueType::Float8),
	(4, ValueType::Int1),
	(5, ValueType::Int2),
	(6, ValueType::Int4),
	(7, ValueType::Int8),
	(8, ValueType::Int16),
	(10, ValueType::Uint1),
	(11, ValueType::Uint2),
	(12, ValueType::Uint4),
	(13, ValueType::Uint8),
	(14, ValueType::Uint16),
	(18, ValueType::Duration),
	(23, ValueType::INT),
	(24, ValueType::UINT),
];

#[derive(Debug, thiserror::Error)]
pub enum DigestError {
	#[error("digest does not support {inner} input")]
	UnsupportedInnerType {
		inner: ValueType,
	},

	#[error("digest accuracy must be between 0.001 and 0.1")]
	AccuracyOutOfRange,

	#[error("digest of {expected} cannot take a {actual} value")]
	InputTypeMismatch {
		expected: ValueType,
		actual: ValueType,
	},

	#[error("digest input cannot have a month part")]
	DurationMonthPart,

	#[error(transparent)]
	DurationConversion(#[from] Box<TypeError>),

	#[error(
		"cannot merge digest({left_inner}, {left_accuracy} ppm) with digest({right_inner}, {right_accuracy} ppm)"
	)]
	MergeMismatch {
		left_inner: ValueType,
		left_accuracy: u32,
		right_inner: ValueType,
		right_accuracy: u32,
	},

	#[error("digest encoding version {version} is unknown")]
	UnknownVersion {
		version: u64,
	},

	#[error("digest encoding ends early")]
	Truncated,

	#[error("digest encoding has {remaining} trailing bytes")]
	TrailingBytes {
		remaining: usize,
	},

	#[error("digest encoding has a varint that is not minimal")]
	NonMinimalVarint,

	#[error("digest encoding has a varint above u64")]
	VarintOverflow,

	#[error("digest encoding has unknown inner type tag {tag}")]
	UnknownInnerTag {
		tag: u64,
	},

	#[error("digest encoding has a bucket index outside i32")]
	BucketIndexOutOfRange,

	#[error("digest encoding repeats bucket index {index}")]
	RepeatedBucketIndex {
		index: i32,
	},

	#[error("digest encoding has a zero count at bucket {index}")]
	ZeroBucketCount {
		index: i32,
	},

	#[error("digest encoding counts overflow u64")]
	CountOverflow,

	#[error("digest of duration cannot hold infinity")]
	NonFiniteDuration,

	#[error("accuracy must be a number")]
	AccuracyNotANumber,

	#[error("accuracy must be a whole number of parts per million")]
	AccuracyNotWholePpm,

	#[error(transparent)]
	PercentileNotANumber(Error),

	#[error("p must be between 0 and 1")]
	PercentileOutOfRange,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Digest {
	inner: ValueType,
	accuracy: u32,
	negative: BTreeMap<i32, u64>,
	zero: u64,
	positive: BTreeMap<i32, u64>,
	negative_infinity: u64,
	positive_infinity: u64,
}

enum Slot {
	NegativeInfinity,
	Negative(i32),
	Zero,
	Positive(i32),
	PositiveInfinity,
}

impl Digest {
	pub fn new(inner: ValueType, accuracy: u32) -> Result<Self, DigestError> {
		if inner_tag(&inner).is_none() {
			return Err(DigestError::UnsupportedInnerType {
				inner,
			});
		}
		if !(MIN_ACCURACY_PPM..=MAX_ACCURACY_PPM).contains(&accuracy) {
			return Err(DigestError::AccuracyOutOfRange);
		}
		Ok(Self {
			inner,
			accuracy,
			negative: BTreeMap::new(),
			zero: 0,
			positive: BTreeMap::new(),
			negative_infinity: 0,
			positive_infinity: 0,
		})
	}

	pub fn inner(&self) -> &ValueType {
		&self.inner
	}

	pub fn accuracy(&self) -> u32 {
		self.accuracy
	}

	pub fn count(&self) -> u64 {
		self.checked_count().expect("digest count overflows u64")
	}

	pub fn bucket_count(&self) -> usize {
		self.negative.len() + self.positive.len()
	}

	pub fn add_value(&mut self, value: &Value) -> Result<(), DigestError> {
		let value = self.input(value)?;
		self.add(value);
		Ok(())
	}

	pub fn remove_value(&mut self, value: &Value) -> Result<(), DigestError> {
		let value = self.input(value)?;
		self.remove(value);
		Ok(())
	}

	pub fn merge(&mut self, other: &Digest) -> Result<(), DigestError> {
		self.check_compatible(other)?;
		increment(&mut self.negative_infinity, other.negative_infinity, "-inf count");
		increment(&mut self.zero, other.zero, "zero count");
		increment(&mut self.positive_infinity, other.positive_infinity, "+inf count");
		for (&index, &count) in &other.negative {
			add_to_store(&mut self.negative, "negative", index, count);
		}
		for (&index, &count) in &other.positive {
			add_to_store(&mut self.positive, "positive", index, count);
		}
		Ok(())
	}

	pub fn unmerge(&mut self, other: &Digest) -> Result<(), DigestError> {
		self.check_compatible(other)?;
		decrement(&mut self.negative_infinity, other.negative_infinity, "-inf count");
		decrement(&mut self.zero, other.zero, "zero count");
		decrement(&mut self.positive_infinity, other.positive_infinity, "+inf count");
		for (&index, &count) in &other.negative {
			remove_from_store(&mut self.negative, "negative", index, count);
		}
		for (&index, &count) in &other.positive {
			remove_from_store(&mut self.positive, "positive", index, count);
		}
		Ok(())
	}

	pub fn percentile(&self, p: f64) -> Option<f64> {
		assert!((0.0..=1.0).contains(&p), "digest percentile p {p} is outside 0 to 1");
		let n = self.count();
		if n == 0 {
			return None;
		}
		let rank = ((p * n as f64).ceil() as u64).clamp(1, n);
		let mut seen = 0u64;
		let mut reached = |count: u64| {
			seen += count;
			seen >= rank
		};
		if reached(self.negative_infinity) {
			return Some(f64::NEG_INFINITY);
		}
		for (&index, &count) in self.negative.iter().rev() {
			if reached(count) {
				return Some(-self.representative(index));
			}
		}
		if reached(self.zero) {
			return Some(0.0);
		}
		for (&index, &count) in &self.positive {
			if reached(count) {
				return Some(self.representative(index));
			}
		}
		assert!(reached(self.positive_infinity), "digest rank {rank} is past count {n}");
		Some(f64::INFINITY)
	}

	pub fn percentile_value(&self, p: f64) -> Result<Value, DigestError> {
		let is_duration = self.inner == ValueType::Duration;
		match (self.percentile(p), is_duration) {
			(None, true) => Ok(Value::none_of(ValueType::Duration)),
			(None, false) => Ok(Value::none_of(ValueType::Float8)),
			(Some(value), true) => Ok(Value::Duration(Duration::from_nanoseconds(value.round() as i64)?)),
			(Some(value), false) => Ok(Value::float8(value)),
		}
	}

	fn checked_count(&self) -> Option<u64> {
		[self.negative_infinity, self.zero, self.positive_infinity]
			.into_iter()
			.chain(self.negative.values().copied())
			.chain(self.positive.values().copied())
			.try_fold(0u64, |total, count| total.checked_add(count))
	}

	fn check_compatible(&self, other: &Digest) -> Result<(), DigestError> {
		if self.inner != other.inner || self.accuracy != other.accuracy {
			return Err(DigestError::MergeMismatch {
				left_inner: self.inner.clone(),
				left_accuracy: self.accuracy,
				right_inner: other.inner.clone(),
				right_accuracy: other.accuracy,
			});
		}
		Ok(())
	}

	fn input(&self, value: &Value) -> Result<f64, DigestError> {
		match (&self.inner, value) {
			(ValueType::Float4, Value::Float4(v)) => Ok(f64::from(v.value())),
			(ValueType::Float8, Value::Float8(v)) => Ok(v.value()),
			(ValueType::Int1, Value::Int1(v)) => Ok(f64::from(*v)),
			(ValueType::Int2, Value::Int2(v)) => Ok(f64::from(*v)),
			(ValueType::Int4, Value::Int4(v)) => Ok(f64::from(*v)),
			(ValueType::Int8, Value::Int8(v)) => Ok(*v as f64),
			(ValueType::Int16, Value::Int16(v)) => Ok(*v as f64),
			(ValueType::Uint1, Value::Uint1(v)) => Ok(f64::from(*v)),
			(ValueType::Uint2, Value::Uint2(v)) => Ok(f64::from(*v)),
			(ValueType::Uint4, Value::Uint4(v)) => Ok(f64::from(*v)),
			(ValueType::Uint8, Value::Uint8(v)) => Ok(*v as f64),
			(ValueType::Uint16, Value::Uint16(v)) => Ok(*v as f64),
			(
				ValueType::Int {
					..
				},
				Value::Int(v),
			) => Ok(v.to_f64()),
			(
				ValueType::Uint {
					..
				},
				Value::Uint(v),
			) => Ok(v.to_f64()),
			(ValueType::Duration, Value::Duration(v)) if v.get_months() != 0 => {
				Err(DigestError::DurationMonthPart)
			}
			(ValueType::Duration, Value::Duration(v)) => Ok(v.as_nanos()? as f64),
			_ => Err(DigestError::InputTypeMismatch {
				expected: self.inner.clone(),
				actual: value.get_type(),
			}),
		}
	}

	fn add(&mut self, value: f64) {
		match self.slot(value) {
			Slot::NegativeInfinity => increment(&mut self.negative_infinity, 1, "-inf count"),
			Slot::Negative(index) => add_to_store(&mut self.negative, "negative", index, 1),
			Slot::Zero => increment(&mut self.zero, 1, "zero count"),
			Slot::Positive(index) => add_to_store(&mut self.positive, "positive", index, 1),
			Slot::PositiveInfinity => increment(&mut self.positive_infinity, 1, "+inf count"),
		}
	}

	fn remove(&mut self, value: f64) {
		match self.slot(value) {
			Slot::NegativeInfinity => decrement(&mut self.negative_infinity, 1, "-inf count"),
			Slot::Negative(index) => remove_from_store(&mut self.negative, "negative", index, 1),
			Slot::Zero => decrement(&mut self.zero, 1, "zero count"),
			Slot::Positive(index) => remove_from_store(&mut self.positive, "positive", index, 1),
			Slot::PositiveInfinity => decrement(&mut self.positive_infinity, 1, "+inf count"),
		}
	}

	fn slot(&self, value: f64) -> Slot {
		assert!(!value.is_nan(), "digest input is NaN");
		if value == f64::INFINITY {
			Slot::PositiveInfinity
		} else if value == f64::NEG_INFINITY {
			Slot::NegativeInfinity
		} else if value == 0.0 {
			Slot::Zero
		} else if value > 0.0 {
			Slot::Positive(self.index(value))
		} else {
			Slot::Negative(self.index(-value))
		}
	}

	fn index(&self, magnitude: f64) -> i32 {
		ceil(log(magnitude) / log(gamma(self.accuracy))) as i32
	}

	fn representative(&self, index: i32) -> f64 {
		let gamma = gamma(self.accuracy);
		(pow(gamma, f64::from(index) - 1.0) * (2.0 * gamma / (gamma + 1.0))).min(f64::MAX)
	}
}

fn gamma(accuracy: u32) -> f64 {
	let accuracy = f64::from(accuracy) / 1_000_000.0;
	(1.0 + accuracy) / (1.0 - accuracy)
}

fn inner_tag(inner: &ValueType) -> Option<u8> {
	INNER_TAGS.iter().find(|(_, ty)| ty == inner).map(|(tag, _)| *tag)
}

fn inner_from_tag(tag: u64) -> Option<ValueType> {
	INNER_TAGS.iter().find(|(t, _)| u64::from(*t) == tag).map(|(_, ty)| ty.clone())
}

fn increment(slot: &mut u64, count: u64, name: &str) {
	let current = *slot;
	*slot = current
		.checked_add(count)
		.unwrap_or_else(|| panic!("digest {name} {current} overflows u64 adding {count}"));
}

fn decrement(slot: &mut u64, count: u64, name: &str) {
	let current = *slot;
	*slot = current
		.checked_sub(count)
		.unwrap_or_else(|| panic!("digest remove of {count} from {name} holding {current}"));
}

fn add_to_store(store: &mut BTreeMap<i32, u64>, name: &str, index: i32, count: u64) {
	let slot = store.entry(index).or_insert(0);
	let current = *slot;
	*slot = current.checked_add(count).unwrap_or_else(|| {
		panic!("digest {name} bucket {index} holding {current} overflows u64 adding {count}")
	});
}

fn remove_from_store(store: &mut BTreeMap<i32, u64>, name: &str, index: i32, count: u64) {
	let Some(slot) = store.get_mut(&index) else {
		panic!("digest remove of {count} from absent {name} bucket {index}");
	};
	let current = *slot;
	let left = current
		.checked_sub(count)
		.unwrap_or_else(|| panic!("digest remove of {count} from {name} bucket {index} holding {current}"));
	if left == 0 {
		store.remove(&index);
	} else {
		*slot = left;
	}
}

#[cfg(test)]
pub mod tests {

	use super::*;
	use crate::value::{constraint::precision::Precision, int::Int, uint::Uint};

	const ACCURACIES: [u32; 3] = [1_000, 10_000, 100_000];

	pub struct Rng(u64);

	impl Rng {
		pub fn new(seed: u64) -> Self {
			Self(seed)
		}

		pub fn next_u64(&mut self) -> u64 {
			self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
			let mut z = self.0;
			z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
			z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
			z ^ (z >> 31)
		}

		pub fn below(&mut self, bound: u64) -> u64 {
			self.next_u64() % bound
		}

		pub fn unit(&mut self) -> f64 {
			(self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
		}
	}

	pub fn float_digest(accuracy: u32) -> Digest {
		Digest::new(ValueType::Float8, accuracy).unwrap()
	}

	pub fn built(accuracy: u32, values: &[f64]) -> Digest {
		let mut digest = float_digest(accuracy);
		for &value in values {
			digest.add(value);
		}
		digest
	}

	fn magnitude(rng: &mut Rng) -> f64 {
		10f64.powf(rng.unit() * 24.0 - 12.0)
	}

	pub fn any_value(rng: &mut Rng) -> f64 {
		match rng.below(40) {
			0 => f64::NEG_INFINITY,
			1 => f64::INFINITY,
			2..=6 => 0.0,
			7..=10 => [1.0, -2.5, 1000.0][rng.below(3) as usize],
			11..=24 => -magnitude(rng),
			_ => magnitude(rng),
		}
	}

	fn finite_value(rng: &mut Rng, sign: i32) -> f64 {
		match (sign, rng.below(10)) {
			(0, 0..=1) => 0.0,
			(0, 2..=5) => -magnitude(rng),
			(-1, _) => -magnitude(rng),
			_ => magnitude(rng),
		}
	}

	fn sorted(values: &[f64]) -> Vec<f64> {
		let mut sorted = values.to_vec();
		sorted.sort_by(f64::total_cmp);
		sorted
	}

	fn rank(p: f64, n: usize) -> usize {
		((p * n as f64).ceil() as usize).max(1)
	}

	fn bucketed(digest: &Digest, value: f64) -> f64 {
		if !value.is_finite() || value == 0.0 {
			value
		} else if value > 0.0 {
			digest.representative(digest.index(value))
		} else {
			-digest.representative(digest.index(-value))
		}
	}

	fn same(left: Option<f64>, right: Option<f64>) -> bool {
		match (left, right) {
			(Some(l), Some(r)) => l.to_bits() == r.to_bits(),
			(None, None) => true,
			_ => false,
		}
	}

	#[test]
	fn new_digest_is_empty() {
		for accuracy in ACCURACIES {
			let digest = float_digest(accuracy);
			assert_eq!(digest.count(), 0);
			assert_eq!(digest.bucket_count(), 0);
			assert_eq!(digest.percentile(0.5), None);
			assert_eq!(digest.accuracy(), accuracy);
			assert_eq!(digest.inner(), &ValueType::Float8);
		}
	}

	#[test]
	fn new_rejects_accuracy_outside_range() {
		for accuracy in [0, 999, 100_001, u32::MAX] {
			assert!(matches!(
				Digest::new(ValueType::Float8, accuracy),
				Err(DigestError::AccuracyOutOfRange)
			));
		}
		assert!(Digest::new(ValueType::Float8, 1_000).is_ok());
		assert!(Digest::new(ValueType::Float8, 100_000).is_ok());
	}

	#[test]
	fn new_rejects_inner_types_outside_ints_floats_and_duration() {
		let rejected = [
			ValueType::Utf8,
			ValueType::Boolean,
			ValueType::Date,
			ValueType::DateTime,
			ValueType::Time,
			ValueType::IdentityId,
			ValueType::Uuid4,
			ValueType::Uuid7,
			ValueType::Blob,
			ValueType::DECIMAL,
			ValueType::int(Precision::new(10)),
			ValueType::uint(Precision::new(10)),
			ValueType::Any,
			ValueType::DictionaryId,
			ValueType::Option(Box::new(ValueType::Int4)),
			ValueType::List(Box::new(ValueType::Float8)),
		];
		for inner in rejected {
			let result = Digest::new(inner.clone(), 10_000);
			assert!(
				matches!(&result, Err(DigestError::UnsupportedInnerType { inner: got }) if *got == inner),
				"{inner} must be rejected"
			);
		}
	}

	#[test]
	fn new_accepts_every_int_float_and_duration_type() {
		for (_, inner) in INNER_TAGS {
			assert_eq!(Digest::new(inner.clone(), 10_000).unwrap().inner(), &inner);
		}
		assert_eq!(INNER_TAGS.len(), 15);
	}

	#[test]
	fn frozen_bucket_formula() {
		// Any change to gamma, the ceil index or the representative changes answers read from stored digests.
		let pins: [(u32, f64, i32, u64); 24] = [
			(1_000, 1.0, 0, 0x3feff7ced916872b),
			(1_000, 0.5, -346, 0x3fe000999322114d),
			(1_000, 2.0, 347, 0x3ffffecacc74c18e),
			(1_000, 1e-9, -10361, 0x3e112f2ee8f1db2a),
			(1_000, 123.456, 2408, 0x405ed63467efc900),
			(1_000, 1e300, 345388, 0x7e37e269c95c4dcc),
			(1_000, 5e-324, -372219, 0x0000000000000001),
			(1_000, f64::MAX, 354892, 0x7fefffffffffffff),
			(10_000, 1.0, 0, 0x3fefae147ae147af),
			(10_000, 0.5, -34, 0x3fe00c9c77c0ac0b),
			(10_000, 2.0, 35, 0x3fffe609cfee00de),
			(10_000, 1e-9, -1036, 0x3e110d4af35d841d),
			(10_000, 123.456, 241, 0x405eafb0b5c737bc),
			(10_000, 1e300, 34538, 0x7e37d4a690e10c14),
			(10_000, 5e-324, -37220, 0x0000000000000001),
			(10_000, f64::MAX, 35488, 0x7fefb5c3f47e6616),
			(100_000, 1.0, 0, 0x3feccccccccccccd),
			(100_000, 0.5, -3, 0x3fdf8c4a51a0ad8f),
			(100_000, 2.0, 4, 0x400011230bde91ab),
			(100_000, 1e-9, -103, 0x3e1052a20b63bdc0),
			(100_000, 123.456, 24, 0x405bc89289578722),
			(100_000, 1e300, 3443, 0x7e3893e644df1e51),
			(100_000, 5e-324, -3709, 0x0000000000000001),
			(100_000, f64::MAX, 3538, 0x7fefffffffffffff),
		];
		for (accuracy, value, index, representative) in pins {
			let digest = float_digest(accuracy);
			assert_eq!(digest.index(value), index, "index of {value:e} at {accuracy} ppm");
			assert_eq!(
				digest.representative(index).to_bits(),
				representative,
				"representative of bucket {index} at {accuracy} ppm"
			);
			assert_eq!(
				built(accuracy, &[-value]).negative,
				BTreeMap::from([(index, 1)]),
				"negative values bucket by magnitude"
			);
		}
	}

	#[test]
	fn values_route_to_their_store() {
		let mut digest = float_digest(10_000);
		for value in [-3.0, -0.0, 0.0, 4.0, f64::NEG_INFINITY, f64::INFINITY] {
			digest.add(value);
		}
		assert_eq!(digest.negative, BTreeMap::from([(digest.index(3.0), 1)]));
		assert_eq!(digest.positive, BTreeMap::from([(digest.index(4.0), 1)]));
		assert_eq!(digest.zero, 2, "-0.0 must count as zero, not as a negative bucket");
		assert_eq!((digest.negative_infinity, digest.positive_infinity), (1, 1));
		assert_eq!(digest.count(), 6);
	}

	#[test]
	fn subnormal_gets_a_bucket_within_index_range() {
		let mut digest = float_digest(1_000);
		for value in [5e-324, -5e-324, f64::MIN_POSITIVE / 2.0] {
			digest.add(value);
			assert!(digest.index(value.abs()).abs() <= 372_220);
		}
		assert_eq!(digest.zero, 0);
		assert_eq!(digest.bucket_count(), 3);
	}

	#[test]
	fn largest_finite_values_read_back_finite_within_accuracy() {
		for accuracy in ACCURACIES {
			let relative = f64::from(accuracy) / 1_000_000.0;
			for value in [f64::MAX, -f64::MAX, f64::MAX / 1.05, -f64::MAX / 1.05] {
				let answer = built(accuracy, &[value]).percentile(1.0).unwrap();
				assert!(answer.is_finite(), "{value:e} at {accuracy} ppm read back as {answer}");
				assert!(
					(answer - value).abs() <= relative * value.abs(),
					"{value:e} at {accuracy} ppm read back as {answer:e}"
				);
			}
		}
	}

	#[test]
	fn one_outlier_adds_one_bucket_and_remove_drops_it() {
		let mut digest = float_digest(10_000);
		for i in 0..1_000 {
			digest.add(100.0 + f64::from(i) / 1_000.0);
		}
		let cluster = digest.clone();
		let before = digest.bucket_count();
		digest.add(1e15);
		assert_eq!(digest.bucket_count(), before + 1);
		digest.add(-1e-15);
		assert_eq!(digest.bucket_count(), before + 2);
		digest.remove(1e15);
		digest.remove(-1e-15);
		assert_eq!(digest.bucket_count(), before);
		assert_eq!(digest, cluster, "an emptied bucket left behind breaks add then remove equality");
	}

	#[test]
	fn add_then_remove_returns_the_empty_digest() {
		let mut digest = float_digest(10_000);
		for value in [5.0, -5.0, 0.0, f64::INFINITY, f64::NEG_INFINITY] {
			digest.add(value);
		}
		for value in [0.0, -5.0, f64::NEG_INFINITY, 5.0, f64::INFINITY] {
			digest.remove(value);
		}
		assert_eq!(digest, float_digest(10_000));
	}

	#[test]
	#[should_panic(expected = "absent positive bucket")]
	fn remove_from_absent_positive_bucket_panics() {
		let mut digest = built(10_000, &[1.0]);
		digest.remove(50.0);
	}

	#[test]
	#[should_panic(expected = "absent negative bucket")]
	fn remove_from_absent_negative_bucket_panics() {
		let mut digest = built(10_000, &[-1.0, 1.0]);
		digest.remove(-50.0);
	}

	#[test]
	#[should_panic(expected = "from +inf count holding 0")]
	fn remove_from_empty_special_count_panics() {
		let mut digest = built(10_000, &[1.0]);
		digest.remove(f64::INFINITY);
	}

	#[test]
	#[should_panic(expected = "digest input is NaN")]
	fn nan_input_panics_instead_of_landing_in_a_bucket() {
		// Without the check NaN takes the negative branch and silently counts as bucket 0.
		let mut digest = built(10_000, &[1.0]);
		digest.add(f64::NAN);
	}

	#[test]
	fn add_value_converts_each_supported_type() {
		let cases = [
			(ValueType::Float4, Value::float4(1.5f32), 1.5),
			(ValueType::Float8, Value::float8(-2.25), -2.25),
			(ValueType::Float8, Value::float8(f64::INFINITY), f64::INFINITY),
			(ValueType::Int1, Value::Int1(-7), -7.0),
			(ValueType::Int2, Value::Int2(300), 300.0),
			(ValueType::Int4, Value::Int4(-70_000), -70_000.0),
			(ValueType::Int8, Value::Int8(1 << 40), (1u64 << 40) as f64),
			(ValueType::Int16, Value::Int16(-(1i128 << 100)), -((1u128 << 100) as f64)),
			(ValueType::Uint1, Value::Uint1(200), 200.0),
			(ValueType::Uint2, Value::Uint2(60_000), 60_000.0),
			(ValueType::Uint4, Value::Uint4(4_000_000_000), 4_000_000_000.0),
			(ValueType::Uint8, Value::Uint8(u64::MAX), u64::MAX as f64),
			(ValueType::Uint16, Value::Uint16(u128::MAX), u128::MAX as f64),
			(ValueType::INT, Value::Int(Int::from_i64(-12_345)), -12_345.0),
			(ValueType::INT, Value::Int(Int::MAX), 1e76),
			(ValueType::INT, Value::Int(Int::MIN), -1e76),
			(ValueType::UINT, Value::Uint(Uint::from_u64(987_654_321)), 987_654_321.0),
			(ValueType::UINT, Value::Uint(Uint::MAX), 1e76),
			(ValueType::Duration, Value::Duration(Duration::from_milliseconds(1_500).unwrap()), 1.5e9),
			(ValueType::Duration, Value::Duration(Duration::from_days(-2).unwrap()), -1.728e14),
		];
		for (inner, value, expected) in cases {
			let mut through_value = Digest::new(inner.clone(), 10_000).unwrap();
			through_value.add_value(&value).unwrap();
			let mut direct = Digest::new(inner.clone(), 10_000).unwrap();
			direct.add(expected);
			assert_eq!(through_value, direct, "{inner} value {value:?}");
			through_value.remove_value(&value).unwrap();
			assert_eq!(through_value.count(), 0, "{inner} value {value:?}");
		}
	}

	#[test]
	fn add_value_rejects_a_value_of_another_type() {
		let mut digest = built(10_000, &[1.0]);
		let before = digest.clone();
		let cases = [
			(Value::Int4(1), ValueType::Int4),
			(Value::utf8("1"), ValueType::Utf8),
			(Value::float4(1.0f32), ValueType::Float4),
			(Value::none_of(ValueType::Float8), ValueType::Option(Box::new(ValueType::Float8))),
		];
		for (value, actual) in cases {
			let added = digest.add_value(&value);
			assert!(
				matches!(&added, Err(DigestError::InputTypeMismatch { expected: ValueType::Float8, actual: got }) if *got == actual),
				"{value:?} added as {added:?}"
			);
			assert!(matches!(digest.remove_value(&value), Err(DigestError::InputTypeMismatch { .. })));
		}
		assert_eq!(digest, before);
	}

	#[test]
	fn duration_with_a_month_part_is_rejected() {
		// as_nanos counts a month as 30 days, so converting one would bucket a guessed length.
		let rejected = [
			Duration::from_months(1).unwrap(),
			Duration::from_months(-1).unwrap(),
			Duration::from_years(1).unwrap(),
			Duration::new(1, 0, 5).unwrap(),
			Duration::new(-1, -2, -5).unwrap(),
			Duration::MAX,
		];
		let mut digest = Digest::new(ValueType::Duration, 10_000).unwrap();
		digest.add_value(&Value::Duration(Duration::from_days(30).unwrap())).unwrap();
		let before = digest.clone();
		for duration in rejected {
			let value = Value::Duration(duration);
			assert!(
				matches!(digest.add_value(&value), Err(DigestError::DurationMonthPart)),
				"add {duration:?}"
			);
			assert!(
				matches!(digest.remove_value(&value), Err(DigestError::DurationMonthPart)),
				"remove {duration:?}"
			);
		}
		assert_eq!(digest, before, "a rejected duration must not touch any count");
	}

	#[test]
	fn duration_with_a_day_part_counts_the_day_as_86400_seconds() {
		// Dropping the day part reads 25h back as 1h, far outside any accuracy.
		for accuracy in ACCURACIES {
			for hours in [25, -25, 24 * 400] {
				let duration = Duration::from_hours(hours).unwrap();
				assert_eq!(duration.get_days(), (hours / 24) as i32, "{hours}h must carry a day part");
				let mut digest = Digest::new(ValueType::Duration, accuracy).unwrap();
				digest.add_value(&Value::Duration(duration)).unwrap();
				let Value::Duration(answer) = digest.percentile_value(0.5).unwrap() else {
					panic!("duration digest must read back a duration");
				};
				let exact = hours as f64 * 3_600e9;
				let nanos = answer.get_days() as f64 * 86_400e9 + answer.get_nanos() as f64;
				assert!(
					(nanos - exact).abs() <= f64::from(accuracy) / 1_000_000.0 * exact.abs(),
					"{hours}h at {accuracy} ppm read back as {answer:?}"
				);
				assert!(answer.get_nanos().abs() < 86_400_000_000_000, "{answer:?} is not normalized");
				digest.remove_value(&Value::Duration(duration)).unwrap();
				assert_eq!(digest.count(), 0, "{hours}h at {accuracy} ppm");
			}
		}
	}

	#[test]
	fn duration_whose_nanoseconds_overflow_i64_fails() {
		// A wrapped or clamped total would count a far-off value instead of failing the query.
		let overflowing = [
			Duration::from_days(106_752).unwrap(),
			Duration::from_days(-106_752).unwrap(),
			Duration::new(0, 106_751, 86_399_999_999_999).unwrap(),
			Duration::from_days(i64::from(i32::MIN)).unwrap(),
		];
		let mut digest = Digest::new(ValueType::Duration, 10_000).unwrap();
		for nanos in [i64::MAX, i64::MIN] {
			digest.add_value(&Value::Duration(Duration::from_nanoseconds(nanos).unwrap())).unwrap();
		}
		let before = digest.clone();
		for duration in overflowing {
			let value = Value::Duration(duration);
			assert!(
				matches!(digest.add_value(&value), Err(DigestError::DurationConversion(_))),
				"add {duration:?}"
			);
			assert!(
				matches!(digest.remove_value(&value), Err(DigestError::DurationConversion(_))),
				"remove {duration:?}"
			);
		}
		assert_eq!(digest, before, "an overflowing duration must not touch any count");
	}

	#[test]
	fn percentile_value_is_float8_or_duration_from_the_inner_type() {
		let mut ints = Digest::new(ValueType::Int4, 10_000).unwrap();
		assert_eq!(ints.percentile_value(0.5).unwrap(), Value::none_of(ValueType::Float8));
		ints.add_value(&Value::Int4(40)).unwrap();
		assert_eq!(ints.percentile_value(0.5).unwrap(), Value::float8(ints.percentile(0.5).unwrap()));

		let mut durations = Digest::new(ValueType::Duration, 10_000).unwrap();
		assert_eq!(durations.percentile_value(0.5).unwrap(), Value::none_of(ValueType::Duration));
		for millis in 1..=100 {
			durations.add_value(&Value::Duration(Duration::from_milliseconds(millis).unwrap())).unwrap();
		}
		let nanos = durations.percentile(0.5).unwrap();
		assert_eq!(
			durations.percentile_value(0.5).unwrap(),
			Value::Duration(Duration::from_nanoseconds(nanos.round() as i64).unwrap())
		);
		assert!((nanos - 50e6).abs() <= 0.01 * 50e6);
	}

	#[test]
	fn percentile_value_saturates_a_duration_representative_past_i64() {
		for accuracy in ACCURACIES {
			let mut digest = Digest::new(ValueType::Duration, accuracy).unwrap();
			for nanos in [i64::MAX, i64::MIN] {
				digest.add_value(&Value::Duration(Duration::from_nanoseconds(nanos).unwrap())).unwrap();
			}
			for (p, nanos) in [(1.0, i64::MAX), (0.0, i64::MIN)] {
				let Value::Duration(answer) = digest.percentile_value(p).unwrap() else {
					panic!("duration digest must read back a duration");
				};
				let answer = answer.as_nanos().unwrap() as f64;
				let exact = nanos as f64;
				assert!((answer - exact).abs() <= f64::from(accuracy) / 1_000_000.0 * exact.abs());
			}
		}
	}

	#[test]
	fn merge_commutes_and_associates() {
		let mut rng = Rng::new(7);
		for accuracy in ACCURACIES {
			let parts: Vec<Vec<f64>> =
				(0..3).map(|_| (0..300).map(|_| any_value(&mut rng)).collect()).collect();
			let [a, b, c] = [0, 1, 2].map(|i| built(accuracy, &parts[i]));

			let mut ab = a.clone();
			ab.merge(&b).unwrap();
			let mut ba = b.clone();
			ba.merge(&a).unwrap();
			assert_eq!(ab, ba);

			let mut ab_c = ab.clone();
			ab_c.merge(&c).unwrap();
			let mut bc = b.clone();
			bc.merge(&c).unwrap();
			let mut a_bc = a.clone();
			a_bc.merge(&bc).unwrap();
			assert_eq!(ab_c, a_bc);

			assert_eq!(ab_c, built(accuracy, &parts.concat()), "merge must equal adding every value");
		}
	}

	#[test]
	fn unmerge_undoes_merge() {
		let mut rng = Rng::new(11);
		for accuracy in ACCURACIES {
			let left: Vec<f64> = (0..400).map(|_| any_value(&mut rng)).collect();
			let right: Vec<f64> = (0..400).map(|_| any_value(&mut rng)).collect();
			let a = built(accuracy, &left);
			let b = built(accuracy, &right);
			let mut total = a.clone();
			total.merge(&b).unwrap();
			total.unmerge(&b).unwrap();
			assert_eq!(total, a, "buckets emptied by unmerge must be dropped");
			total.unmerge(&a).unwrap();
			assert_eq!(total, float_digest(accuracy));
		}
	}

	#[test]
	fn merge_and_unmerge_reject_another_accuracy_or_inner_type() {
		let mut digest = built(10_000, &[1.0, 2.0]);
		let before = digest.clone();
		let other_accuracy = built(20_000, &[1.0]);
		let other_inner = Digest::new(ValueType::Int4, 10_000).unwrap();
		for other in [&other_accuracy, &other_inner] {
			assert!(matches!(digest.merge(other), Err(DigestError::MergeMismatch { .. })));
			assert!(matches!(digest.unmerge(other), Err(DigestError::MergeMismatch { .. })));
		}
		let Err(DigestError::MergeMismatch {
			left_inner,
			left_accuracy,
			right_inner,
			right_accuracy,
		}) = digest.merge(&other_inner)
		else {
			panic!("inner mismatch must be an error");
		};
		assert_eq!(
			(left_inner, left_accuracy, right_inner, right_accuracy),
			(ValueType::Float8, 10_000, ValueType::Int4, 10_000)
		);
		assert_eq!(digest, before);
	}

	#[test]
	#[should_panic(expected = "absent positive bucket")]
	fn unmerge_of_a_part_not_inside_the_total_panics() {
		let mut total = built(10_000, &[1.0, 2.0]);
		total.unmerge(&built(10_000, &[1.0, 3.0])).unwrap();
	}

	#[test]
	#[should_panic(expected = "positive bucket 0 holding 1")]
	fn unmerge_of_more_than_a_bucket_holds_panics() {
		let mut total = built(10_000, &[1.0]);
		total.unmerge(&built(10_000, &[1.0, 1.0])).unwrap();
	}

	#[test]
	fn seeded_add_remove_equals_never_adding_the_removed_values() {
		for seed in 0..20 {
			let mut rng = Rng::new(seed);
			let accuracy = ACCURACIES[seed as usize % 3];
			let mut digest = float_digest(accuracy);
			let mut present = Vec::new();
			for _ in 0..2_000 {
				if !present.is_empty() && rng.below(3) == 0 {
					let at = rng.below(present.len() as u64) as usize;
					digest.remove(present.swap_remove(at));
				} else {
					let value = any_value(&mut rng);
					present.push(value);
					digest.add(value);
				}
			}
			let rebuilt = built(accuracy, &present);
			assert_eq!(digest, rebuilt, "seed {seed}");
			assert_eq!(digest.encode(), rebuilt.encode(), "seed {seed}");
		}
	}

	#[test]
	fn percentile_equals_the_bucketed_oracle() {
		let mut rng = Rng::new(42);
		let ps = [0.0, 0.001, 0.01, 0.07, 0.1, 0.25, 0.333, 0.5, 0.75, 0.9, 0.99, 0.999, 1.0];
		for round in 0..60 {
			let accuracy = ACCURACIES[round % 3];
			let n = [1, 2, 3, 10, 100, 1_000][round % 6] + rng.below(5) as usize;
			let values: Vec<f64> = (0..n).map(|_| any_value(&mut rng)).collect();
			let digest = built(accuracy, &values);
			let sorted = sorted(&values);
			let random_ps: Vec<f64> = (0..20).map(|_| rng.unit()).collect();
			for &p in ps.iter().chain(&random_ps) {
				let expected = Some(bucketed(&digest, sorted[rank(p, n) - 1]));
				let answer = digest.percentile(p);
				assert!(
					same(answer, expected),
					"round {round} n {n} p {p}: {answer:?} != {expected:?}"
				);
			}
		}
	}

	#[test]
	fn rank_is_the_f64_ceil_of_p_times_n() {
		// 0.07 * 100.0 is 7.000000000000001, so the rank is 8; an exact or rounded rank reads the 7th value.
		let values: Vec<f64> = (1..=100).map(f64::from).collect();
		let digest = built(1_000, &values);
		let eighth = digest.representative(digest.index(8.0));
		assert_ne!(eighth, digest.representative(digest.index(7.0)));
		assert_eq!(digest.percentile(0.07), Some(eighth));
		assert_eq!(digest.percentile(0.0), Some(digest.representative(digest.index(1.0))));
		assert_eq!(digest.percentile(1.0), Some(digest.representative(digest.index(100.0))));
	}

	#[test]
	fn percentile_is_within_accuracy_for_positive_negative_and_zero_values() {
		// A wrong bucket or representative misses this bound by up to a whole accuracy, never by rounding.
		let mut rng = Rng::new(3);
		for round in 0..90 {
			let accuracy = ACCURACIES[round % 3];
			let sign = [1, -1, 0][(round / 3) % 3];
			let n = 1 + rng.below(500) as usize;
			let values: Vec<f64> = (0..n).map(|_| finite_value(&mut rng, sign)).collect();
			let digest = built(accuracy, &values);
			let sorted = sorted(&values);
			let relative = f64::from(accuracy) / 1_000_000.0;
			for _ in 0..50 {
				let p = rng.unit();
				let exact = sorted[rank(p, n) - 1];
				let answer = digest.percentile(p).unwrap();
				assert!(
					(answer - exact).abs() <= relative * exact.abs(),
					"round {round} p {p}: {answer:e} is not within {accuracy} ppm of {exact:e}"
				);
			}
		}
	}

	#[test]
	fn ranks_landing_on_infinity_return_it() {
		let digest = built(10_000, &[1.0, f64::INFINITY, 0.0, f64::NEG_INFINITY, -1.0]);
		assert_eq!(digest.percentile(0.0), Some(f64::NEG_INFINITY));
		assert_eq!(digest.percentile(0.1), Some(f64::NEG_INFINITY));
		assert_eq!(digest.percentile(0.3), Some(-digest.representative(digest.index(1.0))));
		assert_eq!(digest.percentile(0.5), Some(0.0));
		assert_eq!(digest.percentile(0.7), Some(digest.representative(digest.index(1.0))));
		assert_eq!(digest.percentile(0.9), Some(f64::INFINITY));
		assert_eq!(digest.percentile(1.0), Some(f64::INFINITY));
	}

	#[test]
	#[should_panic(expected = "outside 0 to 1")]
	fn percentile_outside_zero_to_one_panics() {
		built(10_000, &[1.0]).percentile(1.5);
	}
}
