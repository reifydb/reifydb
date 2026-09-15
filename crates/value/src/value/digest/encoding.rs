// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_bytes::ByteBuf;

use crate::value::{
	digest::{Digest, DigestError, inner_from_tag, inner_tag},
	value_type::ValueType,
};

const VERSION: u64 = 1;

impl Digest {
	pub fn encode(&self) -> Vec<u8> {
		let mut out = Vec::new();
		write_varint(&mut out, VERSION);
		write_varint(&mut out, u64::from(inner_tag(&self.inner).expect("digest inner type has a tag")));
		write_varint(&mut out, u64::from(self.accuracy));
		write_varint(&mut out, self.zero);
		write_varint(&mut out, self.negative_infinity);
		write_varint(&mut out, self.positive_infinity);
		write_store(&mut out, &self.negative);
		write_store(&mut out, &self.positive);
		out
	}

	pub fn decode(bytes: &[u8]) -> Result<Self, DigestError> {
		let mut reader = Reader {
			bytes,
			position: 0,
		};
		let version = reader.varint()?;
		if version != VERSION {
			return Err(DigestError::UnknownVersion {
				version,
			});
		}
		let tag = reader.varint()?;
		let inner = inner_from_tag(tag).ok_or(DigestError::UnknownInnerTag {
			tag,
		})?;
		let accuracy = u32::try_from(reader.varint()?).map_err(|_| DigestError::AccuracyOutOfRange)?;
		let mut digest = Digest::new(inner, accuracy)?;
		digest.zero = reader.varint()?;
		digest.negative_infinity = reader.varint()?;
		digest.positive_infinity = reader.varint()?;
		digest.negative = reader.store()?;
		digest.positive = reader.store()?;
		if reader.position != bytes.len() {
			return Err(DigestError::TrailingBytes {
				remaining: bytes.len() - reader.position,
			});
		}
		if digest.inner == ValueType::Duration
			&& (digest.negative_infinity != 0 || digest.positive_infinity != 0)
		{
			return Err(DigestError::NonFiniteDuration);
		}
		if digest.checked_count().is_none() {
			return Err(DigestError::CountOverflow);
		}
		Ok(digest)
	}
}

impl Serialize for Digest {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_bytes(&self.encode())
	}
}

impl<'de> Deserialize<'de> for Digest {
	fn deserialize<D>(deserializer: D) -> Result<Digest, D::Error>
	where
		D: Deserializer<'de>,
	{
		let bytes = ByteBuf::deserialize(deserializer)?;
		Digest::decode(&bytes).map_err(de::Error::custom)
	}
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
	while value >= 0x80 {
		out.push((value as u8) | 0x80);
		value >>= 7;
	}
	out.push(value as u8);
}

fn write_store(out: &mut Vec<u8>, store: &BTreeMap<i32, u64>) {
	write_varint(out, store.len() as u64);
	let mut previous: Option<i32> = None;
	for (&index, &count) in store {
		match previous {
			None => write_varint(out, zigzag(index)),
			Some(previous) => write_varint(out, (i64::from(index) - i64::from(previous)) as u64),
		}
		write_varint(out, count);
		previous = Some(index);
	}
}

fn zigzag(index: i32) -> u64 {
	u64::from(((index << 1) ^ (index >> 31)) as u32)
}

fn unzigzag(encoded: u32) -> i32 {
	((encoded >> 1) as i32) ^ -((encoded & 1) as i32)
}

struct Reader<'a> {
	bytes: &'a [u8],
	position: usize,
}

impl Reader<'_> {
	fn varint(&mut self) -> Result<u64, DigestError> {
		let mut value = 0u64;
		let mut shift = 0u32;
		loop {
			let byte = *self.bytes.get(self.position).ok_or(DigestError::Truncated)?;
			self.position += 1;
			let low = u64::from(byte & 0x7f);
			if shift == 63 && low > 1 {
				return Err(DigestError::VarintOverflow);
			}
			value |= low << shift;
			if byte & 0x80 == 0 {
				if byte == 0 && shift > 0 {
					return Err(DigestError::NonMinimalVarint);
				}
				return Ok(value);
			}
			shift += 7;
			if shift > 63 {
				return Err(DigestError::VarintOverflow);
			}
		}
	}

	fn store(&mut self) -> Result<BTreeMap<i32, u64>, DigestError> {
		let len = self.varint()?;
		let mut store = BTreeMap::new();
		let mut previous: Option<i32> = None;
		for _ in 0..len {
			let index = match previous {
				None => {
					let encoded = u32::try_from(self.varint()?)
						.map_err(|_| DigestError::BucketIndexOutOfRange)?;
					unzigzag(encoded)
				}
				Some(previous) => {
					let delta = self.varint()?;
					if delta == 0 {
						return Err(DigestError::RepeatedBucketIndex {
							index: previous,
						});
					}
					i64::try_from(delta)
						.ok()
						.and_then(|delta| i64::from(previous).checked_add(delta))
						.and_then(|index| i32::try_from(index).ok())
						.ok_or(DigestError::BucketIndexOutOfRange)?
				}
			};
			let count = self.varint()?;
			if count == 0 {
				return Err(DigestError::ZeroBucketCount {
					index,
				});
			}
			store.insert(index, count);
			previous = Some(index);
		}
		Ok(store)
	}
}

#[cfg(test)]
mod tests {
	use postcard::{from_bytes, to_allocvec};
	use serde_bytes::Bytes;
	use serde_json::{from_str, to_string};

	use super::*;
	use crate::value::digest::{
		INNER_TAGS,
		tests::{Rng, any_value, built, float_digest},
	};

	const EMPTY_FLOAT8: [u64; 8] = [1, 3, 10_000, 0, 0, 0, 0, 0];

	fn varints(values: &[u64]) -> Vec<u8> {
		let mut out = Vec::new();
		for &value in values {
			write_varint(&mut out, value);
		}
		out
	}

	fn header_with(position: usize, value: u64) -> Vec<u64> {
		let mut fields = EMPTY_FLOAT8[..6].to_vec();
		fields[position] = value;
		fields
	}

	fn with_positive(positive: &[u64]) -> Vec<u8> {
		varints(&[&EMPTY_FLOAT8[..7], positive].concat())
	}

	fn with_negative(negative: &[u64]) -> Vec<u8> {
		varints(&[&EMPTY_FLOAT8[..6], negative, &[0]].concat())
	}

	fn sample() -> Digest {
		built(10_000, &[-2.0, 0.0, 0.5, 1.0, 1.0, 2.0, f64::INFINITY])
	}

	fn shuffle(rng: &mut Rng, values: &mut [f64]) {
		for i in (1..values.len()).rev() {
			values.swap(i, rng.below(i as u64 + 1) as usize);
		}
	}

	#[test]
	fn canonical_bytes_are_pinned() {
		// Stored digests are read back with these exact bytes; any layout change needs a new version.
		assert_eq!(
			sample().encode(),
			vec![
				0x01, 0x03, 0x90, 0x4e, 0x01, 0x00, 0x01, 0x01, 0x46, 0x01, 0x03, 0x43, 0x01, 0x22,
				0x02, 0x23, 0x01
			]
		);
		assert_eq!(Digest::decode(&sample().encode()).unwrap(), sample());
		assert_eq!(Digest::decode(&varints(&EMPTY_FLOAT8)).unwrap(), float_digest(10_000));
	}

	#[test]
	fn inner_type_tags_are_the_frozen_type_numbers() {
		let pinned = [
			(ValueType::Float4, 2),
			(ValueType::Float8, 3),
			(ValueType::Int1, 4),
			(ValueType::Int2, 5),
			(ValueType::Int4, 6),
			(ValueType::Int8, 7),
			(ValueType::Int16, 8),
			(ValueType::Uint1, 10),
			(ValueType::Uint2, 11),
			(ValueType::Uint4, 12),
			(ValueType::Uint8, 13),
			(ValueType::Uint16, 14),
			(ValueType::Duration, 18),
			(ValueType::Int, 23),
			(ValueType::Uint, 24),
		];
		for (inner, tag) in pinned {
			let digest = Digest::new(inner.clone(), 10_000).unwrap();
			assert_eq!(digest.encode()[1], tag, "{inner}");
			assert_eq!(Digest::decode(&digest.encode()).unwrap().inner(), &inner);
		}
	}

	#[test]
	fn varint_and_zigzag_are_pinned() {
		let cases: [(u64, &[u8]); 5] = [
			(0, &[0x00]),
			(127, &[0x7f]),
			(128, &[0x80, 0x01]),
			(300, &[0xac, 0x02]),
			(u64::MAX, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
		];
		for (value, bytes) in cases {
			assert_eq!(varints(&[value]), bytes);
			let mut reader = Reader {
				bytes,
				position: 0,
			};
			assert_eq!(reader.varint().unwrap(), value);
		}
		for (index, encoded) in
			[(0, 0), (-1, 1), (1, 2), (-34, 67), (i32::MAX, u32::MAX - 1), (i32::MIN, u32::MAX)]
		{
			assert_eq!(zigzag(index), u64::from(encoded));
			assert_eq!(unzigzag(encoded), index);
		}
	}

	#[test]
	fn round_trip_keeps_inner_type_accuracy_and_every_count() {
		let mut rng = Rng::new(5);
		for (_, inner) in INNER_TAGS {
			for accuracy in [1_000, 12_345, 100_000] {
				let mut digest = Digest::new(inner.clone(), accuracy).unwrap();
				for _ in 0..500 {
					let value = any_value(&mut rng);
					if inner != ValueType::Duration || value.is_finite() {
						digest.add(value);
					}
				}
				let bytes = digest.encode();
				assert_eq!(bytes[0], 1, "the version tag must be the first byte");
				let decoded = Digest::decode(&bytes).unwrap();
				assert_eq!(decoded, digest);
				assert_eq!(decoded.inner(), &inner);
				assert_eq!(decoded.accuracy(), accuracy);
				assert_eq!(decoded.encode(), bytes);
			}
		}
	}

	#[test]
	fn any_add_order_gives_equal_bytes() {
		let mut rng = Rng::new(9);
		for accuracy in [1_000, 10_000, 100_000] {
			let mut values: Vec<f64> = (0..1_000).map(|_| any_value(&mut rng)).collect();
			let bytes = built(accuracy, &values).encode();
			for _ in 0..5 {
				shuffle(&mut rng, &mut values);
				assert_eq!(built(accuracy, &values).encode(), bytes);
			}
			let mut churned = built(accuracy, &values);
			let extra: Vec<f64> = (0..300).map(|_| any_value(&mut rng)).collect();
			for &value in &extra {
				churned.add(value);
			}
			for &value in extra.iter().rev() {
				churned.remove(value);
			}
			assert_eq!(churned.encode(), bytes, "history must not reach the bytes");
		}
	}

	#[test]
	fn decode_rejects_unknown_version() {
		for version in [0, 2, 1 << 40] {
			let bytes = varints(&[&header_with(0, version)[..], &[0, 0]].concat());
			assert!(
				matches!(Digest::decode(&bytes), Err(DigestError::UnknownVersion { version: v }) if v == version)
			);
		}
	}

	#[test]
	fn decode_rejects_trailing_bytes() {
		let mut bytes = sample().encode();
		bytes.push(0x00);
		assert!(matches!(
			Digest::decode(&bytes),
			Err(DigestError::TrailingBytes {
				remaining: 1
			})
		));
	}

	#[test]
	fn decode_rejects_every_truncation() {
		let bytes = sample().encode();
		for len in 0..bytes.len() {
			assert!(
				matches!(Digest::decode(&bytes[..len]), Err(DigestError::Truncated)),
				"prefix of {len} bytes"
			);
		}
		assert!(matches!(Digest::decode(&[0x01, 0x83]), Err(DigestError::Truncated)));
	}

	#[test]
	fn decode_rejects_repeated_bucket_index() {
		assert!(matches!(
			Digest::decode(&with_positive(&[2, 0, 1, 0, 1])),
			Err(DigestError::RepeatedBucketIndex {
				index: 0
			})
		));
		assert!(matches!(
			Digest::decode(&with_negative(&[3, 9, 1, 1, 1, 0, 1])),
			Err(DigestError::RepeatedBucketIndex {
				index: -4
			})
		));
	}

	#[test]
	fn decode_rejects_indexes_that_leave_i32() {
		// Deltas are unsigned, so an unsorted store can only be written as an index that runs past i32.
		assert!(matches!(
			Digest::decode(&with_positive(&[2, zigzag(i32::MAX), 1, 1, 1])),
			Err(DigestError::BucketIndexOutOfRange)
		));
		assert!(matches!(
			Digest::decode(&with_positive(&[2, zigzag(i32::MIN), 1, u64::MAX, 1])),
			Err(DigestError::BucketIndexOutOfRange)
		));
		assert!(matches!(
			Digest::decode(&with_positive(&[1, 1 << 32, 1])),
			Err(DigestError::BucketIndexOutOfRange)
		));
		let widest = Digest::decode(&with_positive(&[2, zigzag(i32::MIN), 1, u64::from(u32::MAX), 1])).unwrap();
		assert_eq!(widest.bucket_count(), 2);
	}

	#[test]
	fn decode_rejects_zero_bucket_count() {
		assert!(matches!(
			Digest::decode(&with_positive(&[1, 0, 0])),
			Err(DigestError::ZeroBucketCount {
				index: 0
			})
		));
		assert!(matches!(
			Digest::decode(&with_negative(&[2, 9, 1, 3, 0])),
			Err(DigestError::ZeroBucketCount {
				index: -2
			})
		));
	}

	#[test]
	fn decode_rejects_non_minimal_varints() {
		let padded_version = [&[0x81, 0x00][..], &varints(&[3, 10_000, 0, 0, 0, 0, 0])].concat();
		let padded_accuracy = [&varints(&[1, 3])[..], &[0x90, 0xce, 0x00], &varints(&[0, 0, 0, 0, 0])].concat();
		let padded_count = [&varints(&EMPTY_FLOAT8[..7])[..], &[0x01, 0x00, 0x81, 0x00]].concat();
		let padded_delta = [&varints(&EMPTY_FLOAT8[..7])[..], &[0x02, 0x00, 0x01, 0x81, 0x00, 0x01]].concat();
		for bytes in [padded_version, padded_accuracy, padded_count, padded_delta] {
			assert!(matches!(Digest::decode(&bytes), Err(DigestError::NonMinimalVarint)), "{bytes:02x?}");
		}
	}

	#[test]
	fn decode_rejects_varints_above_u64() {
		let fields = varints(&EMPTY_FLOAT8[..3]);
		let ten_continued = [&fields[..], &[0xff; 10], &[0x01]].concat();
		let tenth_too_high = [&fields[..], &[0xff; 9], &[0x02]].concat();
		for bytes in [ten_continued, tenth_too_high] {
			assert!(matches!(Digest::decode(&bytes), Err(DigestError::VarintOverflow)), "{bytes:02x?}");
		}
		let largest = varints(&[&header_with(3, u64::MAX)[..], &[0, 0]].concat());
		assert_eq!(Digest::decode(&largest).unwrap().count(), u64::MAX);
	}

	#[test]
	fn decode_rejects_unknown_inner_type_tag() {
		for tag in [0, 1, 9, 15, 25, 32, 63, 255] {
			let bytes = varints(&[&header_with(1, tag)[..], &[0, 0]].concat());
			assert!(
				matches!(Digest::decode(&bytes), Err(DigestError::UnknownInnerTag { tag: t }) if t == tag)
			);
		}
	}

	#[test]
	fn decode_rejects_accuracy_outside_range() {
		for accuracy in [0, 999, 100_001, 1 << 32] {
			let bytes = varints(&[&header_with(2, accuracy)[..], &[0, 0]].concat());
			assert!(matches!(Digest::decode(&bytes), Err(DigestError::AccuracyOutOfRange)), "{accuracy}");
		}
	}

	#[test]
	fn decode_rejects_counts_that_overflow_u64() {
		let specials = varints(&[1, 3, 10_000, u64::MAX, 0, 1, 0, 0]);
		let buckets = with_positive(&[2, 0, u64::MAX, 1, 1]);
		for bytes in [specials, buckets] {
			assert!(matches!(Digest::decode(&bytes), Err(DigestError::CountOverflow)), "{bytes:02x?}");
		}
	}

	#[test]
	fn decode_rejects_infinity_in_a_duration_digest() {
		for position in [4, 5] {
			let mut fields = header_with(position, 1);
			fields[1] = 18;
			let bytes = varints(&[&fields[..], &[0, 0]].concat());
			assert!(matches!(Digest::decode(&bytes), Err(DigestError::NonFiniteDuration)), "{position}");
		}
		let zero = varints(&[1, 18, 10_000, 1, 0, 0, 0, 0]);
		assert_eq!(Digest::decode(&zero).unwrap().count(), 1);
	}

	#[test]
	fn postcard_carries_exactly_the_encoded_bytes() {
		let digest = sample();
		let encoded = digest.encode();
		let postcard_bytes = to_allocvec(&digest).unwrap();
		assert_eq!(postcard_bytes, [&varints(&[encoded.len() as u64])[..], &encoded].concat());
		assert_eq!(from_bytes::<Digest>(&postcard_bytes).unwrap(), digest);

		let framed = to_allocvec(&(7u8, digest.clone(), 9u8)).unwrap();
		assert_eq!(from_bytes::<(u8, Digest, u8)>(&framed).unwrap(), (7, digest, 9));
	}

	#[test]
	fn postcard_rejects_malformed_digest_bytes() {
		let mut bytes = sample().encode();
		bytes[0] = 2;
		let framed = to_allocvec(&Bytes::new(&bytes)).unwrap();
		assert!(from_bytes::<Digest>(&framed).is_err());
	}

	#[test]
	fn json_round_trip() {
		let digest = sample();
		let json = to_string(&digest).unwrap();
		assert_eq!(from_str::<Digest>(&json).unwrap(), digest);
	}
}
