// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{
		Bound,
		Bound::{Excluded, Included, Unbounded},
	},
	ops::{Deref, RangeBounds},
};

use reifydb_type::{
	Blob, Date, DateTime, Decimal, Duration, IdentityId, Int, RowNumber, Time, Uint, Uuid4, Uuid7, Value,
};
use serde::{Deserialize, Serialize};

use crate::{
	interface::{IndexId, SourceId},
	util::{
		CowVec,
		encoding::{binary::decode_binary, keycode::KeySerializer},
	},
};

#[derive(Debug, Clone, PartialOrd, Ord, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedKey(pub CowVec<u8>);

impl Deref for EncodedKey {
	type Target = CowVec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl EncodedKey {
	pub fn new(key: impl Into<Vec<u8>>) -> Self {
		Self(CowVec::new(key.into()))
	}

	/// Create a new builder for constructing an EncodedKey
	pub fn builder() -> EncodedKeyBuilder {
		EncodedKeyBuilder::new()
	}

	pub fn as_bytes(&self) -> &[u8] {
		self.0.as_ref()
	}

	pub fn as_slice(&self) -> &[u8] {
		self.0.as_ref()
	}
}

/// A builder for constructing EncodedKey values using keycode encoding
///
/// This provides a fluent API for building composite keys with proper order-preserving encoding.
///
/// # Example
///
/// ```
/// use reifydb_core::EncodedKey;
///
/// let key = EncodedKey::builder().str("user").u64(42).build();
/// ```
pub struct EncodedKeyBuilder {
	serializer: KeySerializer,
}

impl EncodedKeyBuilder {
	/// Create a new builder
	pub fn new() -> Self {
		Self {
			serializer: KeySerializer::new(),
		}
	}

	/// Create a builder with pre-allocated capacity
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			serializer: KeySerializer::with_capacity(capacity),
		}
	}

	/// Build the EncodedKey
	pub fn build(self) -> EncodedKey {
		self.serializer.to_encoded_key()
	}

	/// Extend with bool value
	pub fn bool(mut self, value: bool) -> Self {
		self.serializer.extend_bool(value);
		self
	}

	/// Extend with f32 value
	pub fn f32(mut self, value: f32) -> Self {
		self.serializer.extend_f32(value);
		self
	}

	/// Extend with f64 value
	pub fn f64(mut self, value: f64) -> Self {
		self.serializer.extend_f64(value);
		self
	}

	/// Extend with i8 value
	pub fn i8<T: Into<i8>>(mut self, value: T) -> Self {
		self.serializer.extend_i8(value);
		self
	}

	/// Extend with i16 value
	pub fn i16<T: Into<i16>>(mut self, value: T) -> Self {
		self.serializer.extend_i16(value);
		self
	}

	/// Extend with i32 value
	pub fn i32<T: Into<i32>>(mut self, value: T) -> Self {
		self.serializer.extend_i32(value);
		self
	}

	/// Extend with i64 value
	pub fn i64<T: Into<i64>>(mut self, value: T) -> Self {
		self.serializer.extend_i64(value);
		self
	}

	/// Extend with i128 value
	pub fn i128<T: Into<i128>>(mut self, value: T) -> Self {
		self.serializer.extend_i128(value);
		self
	}

	/// Extend with u8 value
	pub fn u8<T: Into<u8>>(mut self, value: T) -> Self {
		self.serializer.extend_u8(value);
		self
	}

	/// Extend with u16 value
	pub fn u16<T: Into<u16>>(mut self, value: T) -> Self {
		self.serializer.extend_u16(value);
		self
	}

	/// Extend with u32 value
	pub fn u32<T: Into<u32>>(mut self, value: T) -> Self {
		self.serializer.extend_u32(value);
		self
	}

	/// Extend with u64 value
	pub fn u64<T: Into<u64>>(mut self, value: T) -> Self {
		self.serializer.extend_u64(value);
		self
	}

	/// Extend with u128 value
	pub fn u128<T: Into<u128>>(mut self, value: T) -> Self {
		self.serializer.extend_u128(value);
		self
	}

	/// Extend with raw bytes (with encoding)
	pub fn bytes<T: AsRef<[u8]>>(mut self, bytes: T) -> Self {
		self.serializer.extend_bytes(bytes);
		self
	}

	/// Extend with string (UTF-8 bytes)
	pub fn str<T: AsRef<str>>(mut self, s: T) -> Self {
		self.serializer.extend_str(s);
		self
	}

	/// Extend with a SourceId value
	pub fn source_id(mut self, source: impl Into<SourceId>) -> Self {
		self.serializer.extend_source_id(source);
		self
	}

	/// Extend with an IndexId value
	pub fn index_id(mut self, index: impl Into<IndexId>) -> Self {
		self.serializer.extend_index_id(index);
		self
	}

	/// Extend with a serializable value using keycode encoding
	pub fn serialize<T: Serialize>(mut self, value: &T) -> Self {
		self.serializer.extend_serialize(value);
		self
	}

	/// Extend with raw bytes (no encoding)
	pub fn raw(mut self, bytes: &[u8]) -> Self {
		self.serializer.extend_raw(bytes);
		self
	}

	/// Get current buffer length
	pub fn len(&self) -> usize {
		self.serializer.len()
	}

	/// Check if buffer is empty
	pub fn is_empty(&self) -> bool {
		self.serializer.is_empty()
	}

	/// Extend with Date value
	pub fn date(mut self, date: &Date) -> Self {
		self.serializer.extend_date(date);
		self
	}

	/// Extend with DateTime value
	pub fn datetime(mut self, datetime: &DateTime) -> Self {
		self.serializer.extend_datetime(datetime);
		self
	}

	/// Extend with Time value
	pub fn time(mut self, time: &Time) -> Self {
		self.serializer.extend_time(time);
		self
	}

	/// Extend with Duration value
	pub fn duration(mut self, duration: &Duration) -> Self {
		self.serializer.extend_duration(duration);
		self
	}

	/// Extend with RowNumber value
	pub fn row_number(mut self, row_number: &RowNumber) -> Self {
		self.serializer.extend_row_number(row_number);
		self
	}

	/// Extend with IdentityId value
	pub fn identity_id(mut self, id: &IdentityId) -> Self {
		self.serializer.extend_identity_id(id);
		self
	}

	/// Extend with Uuid4 value
	pub fn uuid4(mut self, uuid: &Uuid4) -> Self {
		self.serializer.extend_uuid4(uuid);
		self
	}

	/// Extend with Uuid7 value
	pub fn uuid7(mut self, uuid: &Uuid7) -> Self {
		self.serializer.extend_uuid7(uuid);
		self
	}

	/// Extend with Blob value
	pub fn blob(mut self, blob: &Blob) -> Self {
		self.serializer.extend_blob(blob);
		self
	}

	/// Extend with arbitrary precision Int value
	pub fn int(mut self, int: &Int) -> Self {
		self.serializer.extend_int(int);
		self
	}

	/// Extend with arbitrary precision Uint value
	pub fn uint(mut self, uint: &Uint) -> Self {
		self.serializer.extend_uint(uint);
		self
	}

	/// Extend with Decimal value
	pub fn decimal(mut self, decimal: &Decimal) -> Self {
		self.serializer.extend_decimal(decimal);
		self
	}

	/// Extend with a Value based on its type
	pub fn value(mut self, value: &Value) -> Self {
		self.serializer.extend_value(value);
		self
	}
}

impl Default for EncodedKeyBuilder {
	fn default() -> Self {
		Self::new()
	}
}

/// Trait for types that can be converted into an EncodedKey.
/// Provides convenient conversions from common types to EncodedKey using proper order-preserving encoding.
pub trait IntoEncodedKey {
	fn into_encoded_key(self) -> EncodedKey;
}

// Direct passthrough for EncodedKey
impl IntoEncodedKey for EncodedKey {
	fn into_encoded_key(self) -> EncodedKey {
		self
	}
}

// String types - using extend_str for proper encoding
impl IntoEncodedKey for &str {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for String {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(&self);
		serializer.to_encoded_key()
	}
}

// Byte arrays - using extend_bytes for escaped encoding
impl IntoEncodedKey for Vec<u8> {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_bytes(&self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for &[u8] {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_bytes(self);
		serializer.to_encoded_key()
	}
}

// Numeric types - using proper encoding for order preservation
impl IntoEncodedKey for u64 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(8);
		serializer.extend_u64(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for i64 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(8);
		serializer.extend_i64(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for u32 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(4);
		serializer.extend_u32(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for i32 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(4);
		serializer.extend_i32(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for u16 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u16(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for i16 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_i16(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for u8 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for i8 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_i8(self);
		serializer.to_encoded_key()
	}
}

#[derive(Clone, Debug)]
pub struct EncodedKeyRange {
	pub start: Bound<EncodedKey>,
	pub end: Bound<EncodedKey>,
}

impl EncodedKeyRange {
	pub fn new(start: Bound<EncodedKey>, end: Bound<EncodedKey>) -> Self {
		Self {
			start,
			end,
		}
	}

	/// Generates a key range for a key prefix, used e.g. for prefix scans.
	///
	/// The exclusive end bound is generated by adding 1 to the value of the
	/// last byte. If the last byte(s) is 0xff (so adding 1 would
	/// saturation), we instead find the latest non-0xff byte, increment
	/// that, and truncate the rest. If all bytes are 0xff, we scan to the
	/// end of the range, since there can't be other prefixes after it.
	pub fn prefix(prefix: &[u8]) -> Self {
		let start = Bound::Included(EncodedKey::new(prefix));
		let end = match prefix.iter().rposition(|&b| b != 0xff) {
			Some(i) => Bound::Excluded(EncodedKey::new(
				prefix.iter()
					.take(i)
					.copied()
					.chain(std::iter::once(prefix[i] + 1))
					.collect::<Vec<_>>(),
			)),
			None => Bound::Unbounded,
		};
		Self {
			start,
			end,
		}
	}

	pub fn with_prefix(&self, prefix: EncodedKey) -> Self {
		let start = match self.start_bound() {
			Included(key) => {
				let mut prefixed = Vec::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Included(EncodedKey::new(prefixed))
			}
			Excluded(key) => {
				let mut prefixed = Vec::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Excluded(EncodedKey::new(prefixed))
			}
			Unbounded => Included(prefix.clone()),
		};

		let end = match self.end_bound() {
			Included(key) => {
				let mut prefixed = Vec::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Included(EncodedKey::new(prefixed))
			}
			Excluded(key) => {
				let mut prefixed = Vec::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Excluded(EncodedKey::new(prefixed))
			}
			Unbounded => match prefix.as_ref().iter().rposition(|&b| b != 0xff) {
				Some(i) => {
					let mut next_prefix = prefix.as_ref()[..=i].to_vec();
					next_prefix[i] += 1;
					Excluded(EncodedKey::new(next_prefix))
				}
				None => Unbounded,
			},
		};

		EncodedKeyRange::new(start, end)
	}

	/// Constructs a key range from an optional inclusive start key to an
	/// optional inclusive end key.
	///
	/// - `start`: If provided, marks the inclusive lower bound of the range. If `None`, the range is unbounded
	///   below.
	/// - `end`: If provided, marks the inclusive upper bound of the range. If `None`, the range is unbounded above.
	///
	/// This function does not modify the input keys and assumes they are
	/// already exact keys (not prefixes). If you need to scan all keys
	/// with a given prefix, use [`EncodedKeyRange::prefix`] instead.
	///
	/// Useful for scanning between two explicit keys in a sorted key-value
	/// store.
	pub fn start_end(start: Option<EncodedKey>, end: Option<EncodedKey>) -> Self {
		let start = match start {
			Some(s) => Bound::Included(s),
			None => Bound::Unbounded,
		};

		let end = match end {
			Some(e) => Bound::Included(e),
			None => Bound::Unbounded,
		};

		Self {
			start,
			end,
		}
	}

	/// Constructs a key range that fragments the entire keyspace.
	///
	/// This range has no lower or upper bounds, making it suitable for full
	/// scans over all keys in a sorted key-value store.
	///
	/// Equivalent to: `..` (in Rust range syntax)
	pub fn all() -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}

	/// Parses a human-readable range string into a `KeyRange`.
	///
	/// The expected format is `<start>..[=]<end>`, where:
	/// - `<start>` is the inclusive lower bound (optional),
	/// - `..` separates the bounds,
	/// - `=` after `..` makes the upper bound inclusive,
	/// - `<end>` is the upper bound (optional).
	///
	/// Examples:
	/// - `"a..z"`       => start = Included("a"), end = Excluded("z")
	/// - `"a..=z"`      => start = Included("a"), end = Included("z")
	/// - `"..z"`        => start = Unbounded,     end = Excluded("z")
	/// - `"a.."`        => start = Included("a"), end = Unbounded
	///
	/// If parsing fails, it defaults to a degenerate range from `0xff` to
	/// `0xff` (empty).
	pub fn parse(str: &str) -> Self {
		let (mut start, mut end) = (Bound::<EncodedKey>::Unbounded, Bound::<EncodedKey>::Unbounded);

		// Find the ".." separator
		if let Some(dot_pos) = str.find("..") {
			let start_part = &str[..dot_pos];
			let end_part = &str[dot_pos + 2..];

			// Parse start bound
			if !start_part.is_empty() {
				start = Bound::Included(EncodedKey(decode_binary(start_part)));
			}

			// Parse end bound - check for inclusive marker "="
			if let Some(end_str) = end_part.strip_prefix('=') {
				// Inclusive end: "..="
				if !end_str.is_empty() {
					end = Bound::Included(EncodedKey(decode_binary(end_str)));
				}
			} else {
				// Exclusive end: ".."
				if !end_part.is_empty() {
					end = Bound::Excluded(EncodedKey(decode_binary(end_part)));
				}
			}

			Self {
				start,
				end,
			}
		} else {
			// Invalid format - return degenerate range
			Self {
				start: Bound::Included(EncodedKey::new([0xff])),
				end: Bound::Excluded(EncodedKey::new([0xff])),
			}
		}
	}
}

impl RangeBounds<EncodedKey> for EncodedKeyRange {
	fn start_bound(&self) -> Bound<&EncodedKey> {
		self.start.as_ref()
	}

	fn end_bound(&self) -> Bound<&EncodedKey> {
		self.end.as_ref()
	}
}

#[cfg(test)]
mod tests {
	use std::collections::Bound;

	use super::EncodedKey;

	macro_rules! as_key {
		($key:expr) => {{ EncodedKey::new(keycode::serialize(&$key)) }};
	}

	mod prefix {
		use std::ops::Bound;

		use crate::value::encoded::key::{
			EncodedKeyRange,
			tests::{excluded, included},
		};

		#[test]
		fn test_simple() {
			let range = EncodedKeyRange::prefix(&[0x12, 0x34]);
			assert_eq!(range.start, included(&[0x12, 0x34]));
			assert_eq!(range.end, excluded(&[0x12, 0x35]));
		}

		#[test]
		fn test_with_trailing_ff() {
			let range = EncodedKeyRange::prefix(&[0x12, 0xff]);
			assert_eq!(range.start, included(&[0x12, 0xff]));
			assert_eq!(range.end, excluded(&[0x13]));
		}

		#[test]
		fn test_with_multiple_trailing_ff() {
			let range = EncodedKeyRange::prefix(&[0x12, 0xff, 0xff]);
			assert_eq!(range.start, included(&[0x12, 0xff, 0xff]));
			assert_eq!(range.end, excluded(&[0x13]));
		}

		#[test]
		fn test_all_ff() {
			let range = EncodedKeyRange::prefix(&[0xff, 0xff]);
			assert_eq!(range.start, included(&[0xff, 0xff]));
			assert_eq!(range.end, Bound::Unbounded);
		}

		#[test]
		fn test_empty() {
			let range = EncodedKeyRange::prefix(&[]);
			assert_eq!(range.start, included(&[]));
			assert_eq!(range.end, Bound::Unbounded);
		}

		#[test]
		fn test_mid_increment() {
			let range = EncodedKeyRange::prefix(&[0x12, 0x00, 0xff]);
			assert_eq!(range.start, included(&[0x12, 0x00, 0xff]));
			assert_eq!(range.end, excluded(&[0x12, 0x01]));
		}
	}

	mod start_end {
		use std::ops::Bound;

		use crate::{
			EncodedKey,
			util::encoding::keycode,
			value::encoded::key::{EncodedKeyRange, tests::included},
		};

		#[test]
		fn test_start_and_end() {
			let range = EncodedKeyRange::start_end(Some(as_key!(1)), Some(as_key!(2)));
			assert_eq!(range.start, included(&as_key!(1)));
			assert_eq!(range.end, included(&as_key!(2)));
		}

		#[test]
		fn test_start_only() {
			let range = EncodedKeyRange::start_end(Some(as_key!(1)), None);
			assert_eq!(range.start, included(&as_key!(1)));
			assert_eq!(range.end, Bound::Unbounded);
		}

		#[test]
		fn test_end_only() {
			let range = EncodedKeyRange::start_end(None, Some(as_key!(2)));
			assert_eq!(range.start, Bound::Unbounded);
			assert_eq!(range.end, included(&as_key!(2)));
		}

		#[test]
		fn test_unbounded_range() {
			let range = EncodedKeyRange::start_end(None, None);
			assert_eq!(range.start, Bound::Unbounded);
			assert_eq!(range.end, Bound::Unbounded);
		}

		#[test]
		fn test_full_byte_range() {
			let range = EncodedKeyRange::start_end(Some(as_key!(0x00)), Some(as_key!(0xff)));
			assert_eq!(range.start, included(&as_key!(0x00)));
			assert_eq!(range.end, included(&as_key!(0xff)));
		}

		#[test]
		fn test_identical_bounds() {
			let range = EncodedKeyRange::start_end(Some(as_key!(0x42)), Some(as_key!(0x42)));
			assert_eq!(range.start, included(&as_key!(0x42)));
			assert_eq!(range.end, included(&as_key!(0x42)));
		}
	}

	mod all {
		use std::ops::Bound;

		use crate::value::encoded::key::EncodedKeyRange;

		#[test]
		fn test_is_unbounded() {
			let range = EncodedKeyRange::all();
			assert_eq!(range.start, Bound::Unbounded);
			assert_eq!(range.end, Bound::Unbounded);
		}
	}

	mod parse {
		use std::ops::Bound;

		use crate::value::encoded::key::{
			EncodedKey, EncodedKeyRange,
			tests::{excluded, included},
		};

		#[test]
		fn test_full_range() {
			let r = EncodedKeyRange::parse("a..z");
			assert_eq!(r.start, included(b"a"));
			assert_eq!(r.end, excluded(b"z"));
		}

		#[test]
		fn test_inclusive_end() {
			let r = EncodedKeyRange::parse("a..=z");
			assert_eq!(r.start, included(b"a"));
			assert_eq!(r.end, included(b"z"));
		}

		#[test]
		fn test_unbounded_start() {
			let r = EncodedKeyRange::parse("..z");
			assert_eq!(r.start, Bound::Unbounded);
			assert_eq!(r.end, excluded(b"z"));
		}

		#[test]
		fn test_unbounded_end() {
			let r = EncodedKeyRange::parse("a..");
			assert_eq!(r.start, included(b"a"));
			assert_eq!(r.end, Bound::Unbounded);
		}

		#[test]
		fn test_inclusive_only() {
			let r = EncodedKeyRange::parse("..=z");
			assert_eq!(r.start, Bound::Unbounded);
			assert_eq!(r.end, included(b"z"));
		}

		#[test]
		fn test_invalid_string_returns_degenerate_range() {
			let r = EncodedKeyRange::parse("not a range");
			let expected = EncodedKey::new([0xff]);
			assert_eq!(r.start, Bound::Included(expected.clone()));
			assert_eq!(r.end, Bound::Excluded(expected));
		}

		#[test]
		fn test_empty_string_returns_degenerate_range() {
			let r = EncodedKeyRange::parse("");
			let expected = EncodedKey::new([0xff]);
			assert_eq!(r.start, Bound::Included(expected.clone()));
			assert_eq!(r.end, Bound::Excluded(expected));
		}

		#[test]
		fn test_binary_encoded_values() {
			let r = EncodedKeyRange::parse("0101..=0aff");
			// decode_binary("0101") = [0x01, 0x01]
			assert_eq!(r.start, included(b"0101"));
			// decode_binary("0aff") = [0x0a, 0xff]
			assert_eq!(r.end, included(b"0aff"));
		}
	}

	fn included(key: &[u8]) -> Bound<EncodedKey> {
		Bound::Included(EncodedKey::new(key))
	}

	fn excluded(key: &[u8]) -> Bound<EncodedKey> {
		Bound::Excluded(EncodedKey::new(key))
	}
}
