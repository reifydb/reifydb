// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Borrow,
	cmp::Ordering,
	collections::{
		Bound,
		Bound::{Excluded, Included, Unbounded},
	},
	fmt,
	hash::{Hash, Hasher},
	iter, mem,
	ops::{Deref, RangeBounds},
	sync::Arc,
};

use reifydb_value::{
	util::hash::Hash128,
	value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		row_number::RowNumber,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use serde::{
	de::{Deserialize, Deserializer},
	ser::{Serialize, Serializer},
};

use crate::key::serializer::KeySerializer;

fn decode_binary(s: &str) -> Vec<u8> {
	let mut buf = [0; 4];
	let mut bytes = Vec::new();
	for c in s.chars() {
		match c as u32 {
			b @ 0x80..=0xff => bytes.push(b as u8),
			_ => bytes.extend(c.encode_utf8(&mut buf).as_bytes()),
		}
	}
	bytes
}

#[derive(Clone)]
pub enum EncodedKey {
	Inline {
		len: u8,
		buf: [u8; 62],
	},
	Heap(Vec<u8>),
}

const _: () = assert!(mem::size_of::<EncodedKey>() == 64);

impl EncodedKey {
	const INLINE_CAP: usize = 62;

	pub fn new(key: impl Into<Vec<u8>>) -> Self {
		let vec = key.into();
		if vec.len() <= Self::INLINE_CAP {
			let len = vec.len() as u8;
			let mut buf = [0u8; 62];
			buf[..vec.len()].copy_from_slice(&vec);
			EncodedKey::Inline {
				len,
				buf,
			}
		} else {
			EncodedKey::Heap(vec)
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		if capacity <= Self::INLINE_CAP {
			EncodedKey::Inline {
				len: 0,
				buf: [0u8; 62],
			}
		} else {
			EncodedKey::Heap(Vec::with_capacity(capacity))
		}
	}

	pub fn builder() -> EncodedKeyBuilder {
		EncodedKeyBuilder::new()
	}

	pub fn as_bytes(&self) -> &[u8] {
		self.as_slice()
	}

	pub fn as_slice(&self) -> &[u8] {
		match self {
			EncodedKey::Inline {
				len,
				buf,
			} => &buf[..*len as usize],
			EncodedKey::Heap(v) => v.as_slice(),
		}
	}

	pub fn to_vec(&self) -> Vec<u8> {
		self.as_slice().to_vec()
	}

	pub fn push(&mut self, byte: u8) {
		match self {
			EncodedKey::Inline {
				len,
				buf,
			} => {
				let cur = *len as usize;
				if cur < Self::INLINE_CAP {
					buf[cur] = byte;
					*len += 1;
					return;
				}
				let mut vec = Vec::with_capacity(cur + 1);
				vec.extend_from_slice(&buf[..cur]);
				vec.push(byte);
				*self = EncodedKey::Heap(vec);
			}
			EncodedKey::Heap(v) => v.push(byte),
		}
	}

	pub fn extend_from_slice(&mut self, slice: &[u8]) {
		match self {
			EncodedKey::Inline {
				len,
				buf,
			} => {
				let cur = *len as usize;
				let total = cur + slice.len();
				if total <= Self::INLINE_CAP {
					buf[cur..total].copy_from_slice(slice);
					*len = total as u8;
					return;
				}
				let mut vec = Vec::with_capacity(total);
				vec.extend_from_slice(&buf[..cur]);
				vec.extend_from_slice(slice);
				*self = EncodedKey::Heap(vec);
			}
			EncodedKey::Heap(v) => v.extend_from_slice(slice),
		}
	}
}

impl Deref for EncodedKey {
	type Target = [u8];

	fn deref(&self) -> &[u8] {
		self.as_slice()
	}
}

impl AsRef<[u8]> for EncodedKey {
	fn as_ref(&self) -> &[u8] {
		self.as_slice()
	}
}

impl Borrow<[u8]> for EncodedKey {
	fn borrow(&self) -> &[u8] {
		self.as_slice()
	}
}

impl PartialEq for EncodedKey {
	fn eq(&self, other: &Self) -> bool {
		self.as_slice() == other.as_slice()
	}
}

impl Eq for EncodedKey {}

impl PartialOrd for EncodedKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for EncodedKey {
	fn cmp(&self, other: &Self) -> Ordering {
		self.as_slice().cmp(other.as_slice())
	}
}

impl Hash for EncodedKey {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.as_slice().hash(state);
	}
}

impl PartialEq<Vec<u8>> for EncodedKey {
	fn eq(&self, other: &Vec<u8>) -> bool {
		self.as_slice() == other.as_slice()
	}
}

impl PartialEq<[u8]> for EncodedKey {
	fn eq(&self, other: &[u8]) -> bool {
		self.as_slice() == other
	}
}

impl Serialize for EncodedKey {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.as_slice().serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for EncodedKey {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let vec = Vec::<u8>::deserialize(deserializer)?;
		Ok(EncodedKey::new(vec))
	}
}

impl fmt::Debug for EncodedKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "EncodedKey({:02x?})", self.as_slice())
	}
}

pub struct EncodedKeyBuilder {
	serializer: KeySerializer,
}

impl EncodedKeyBuilder {
	pub fn new() -> Self {
		Self {
			serializer: KeySerializer::new(),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			serializer: KeySerializer::with_capacity(capacity),
		}
	}

	pub fn build(self) -> EncodedKey {
		self.serializer.to_encoded_key()
	}

	pub fn bool(mut self, value: bool) -> Self {
		self.serializer.extend_bool(value);
		self
	}

	pub fn f32(mut self, value: f32) -> Self {
		self.serializer.extend_f32(value);
		self
	}

	pub fn f64(mut self, value: f64) -> Self {
		self.serializer.extend_f64(value);
		self
	}

	pub fn i8<T: Into<i8>>(mut self, value: T) -> Self {
		self.serializer.extend_i8(value);
		self
	}

	pub fn i16<T: Into<i16>>(mut self, value: T) -> Self {
		self.serializer.extend_i16(value);
		self
	}

	pub fn i32<T: Into<i32>>(mut self, value: T) -> Self {
		self.serializer.extend_i32(value);
		self
	}

	pub fn i64<T: Into<i64>>(mut self, value: T) -> Self {
		self.serializer.extend_i64(value);
		self
	}

	pub fn i128<T: Into<i128>>(mut self, value: T) -> Self {
		self.serializer.extend_i128(value);
		self
	}

	pub fn u8<T: Into<u8>>(mut self, value: T) -> Self {
		self.serializer.extend_u8(value);
		self
	}

	pub fn u16<T: Into<u16>>(mut self, value: T) -> Self {
		self.serializer.extend_u16(value);
		self
	}

	pub fn u32<T: Into<u32>>(mut self, value: T) -> Self {
		self.serializer.extend_u32(value);
		self
	}

	pub fn u64<T: Into<u64>>(mut self, value: T) -> Self {
		self.serializer.extend_u64(value);
		self
	}

	pub fn u128<T: Into<u128>>(mut self, value: T) -> Self {
		self.serializer.extend_u128(value);
		self
	}

	pub fn bytes<T: AsRef<[u8]>>(mut self, bytes: T) -> Self {
		self.serializer.extend_bytes(bytes);
		self
	}

	pub fn str<T: AsRef<str>>(mut self, s: T) -> Self {
		self.serializer.extend_str(s);
		self
	}

	pub fn serialize<T: Serialize>(mut self, value: &T) -> Self {
		self.serializer.extend_serialize(value);
		self
	}

	pub fn raw(mut self, bytes: &[u8]) -> Self {
		self.serializer.extend_raw(bytes);
		self
	}

	pub fn len(&self) -> usize {
		self.serializer.len()
	}

	pub fn is_empty(&self) -> bool {
		self.serializer.is_empty()
	}

	pub fn date(mut self, date: &Date) -> Self {
		self.serializer.extend_date(date);
		self
	}

	pub fn datetime(mut self, datetime: &DateTime) -> Self {
		self.serializer.extend_datetime(datetime);
		self
	}

	pub fn time(mut self, time: &Time) -> Self {
		self.serializer.extend_time(time);
		self
	}

	pub fn duration(mut self, duration: &Duration) -> Self {
		self.serializer.extend_duration(duration);
		self
	}

	pub fn row_number(mut self, row_number: &RowNumber) -> Self {
		self.serializer.extend_row_number(row_number);
		self
	}

	pub fn identity_id(mut self, id: &IdentityId) -> Self {
		self.serializer.extend_identity_id(id);
		self
	}

	pub fn uuid4(mut self, uuid: &Uuid4) -> Self {
		self.serializer.extend_uuid4(uuid);
		self
	}

	pub fn uuid7(mut self, uuid: &Uuid7) -> Self {
		self.serializer.extend_uuid7(uuid);
		self
	}

	pub fn blob(mut self, blob: &Blob) -> Self {
		self.serializer.extend_blob(blob);
		self
	}

	pub fn int(mut self, int: &Int) -> Self {
		self.serializer.extend_int(int);
		self
	}

	pub fn uint(mut self, uint: &Uint) -> Self {
		self.serializer.extend_uint(uint);
		self
	}

	pub fn decimal(mut self, decimal: &Decimal) -> Self {
		self.serializer.extend_decimal(decimal);
		self
	}

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

pub trait IntoEncodedKey {
	fn into_encoded_key(self) -> EncodedKey;
}

impl IntoEncodedKey for EncodedKey {
	fn into_encoded_key(self) -> EncodedKey {
		self
	}
}

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

impl IntoEncodedKey for &u64 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &i64 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &u32 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &i32 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &u16 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &i16 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &u8 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &i8 {
	fn into_encoded_key(self) -> EncodedKey {
		(*self).into_encoded_key()
	}
}

impl IntoEncodedKey for &Hash128 {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(16);
		serializer.extend_u128(self.0);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for RowNumber {
	fn into_encoded_key(self) -> EncodedKey {
		self.0.into_encoded_key()
	}
}

impl IntoEncodedKey for &RowNumber {
	fn into_encoded_key(self) -> EncodedKey {
		self.0.into_encoded_key()
	}
}

impl IntoEncodedKey for Value {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for &Value {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_value(self);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for Vec<Value> {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		for value in self.iter() {
			serializer.extend_value(value);
		}
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for &[Value] {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		for value in self.iter() {
			serializer.extend_value(value);
		}
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for (String, String) {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(&self.0);
		serializer.extend_str(&self.1);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for &(String, String) {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(&self.0);
		serializer.extend_str(&self.1);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for (&str, &str) {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(self.0);
		serializer.extend_str(self.1);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for (Arc<str>, Arc<str>) {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(&self.0);
		serializer.extend_str(&self.1);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for &(Arc<str>, Arc<str>) {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(&self.0);
		serializer.extend_str(&self.1);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for (String, String, String) {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(&self.0);
		serializer.extend_str(&self.1);
		serializer.extend_str(&self.2);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for &(String, String, String) {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(&self.0);
		serializer.extend_str(&self.1);
		serializer.extend_str(&self.2);
		serializer.to_encoded_key()
	}
}

impl IntoEncodedKey for &String {
	fn into_encoded_key(self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(self);
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

	pub fn prefix(prefix: &[u8]) -> Self {
		let start = Bound::Included(EncodedKey::new(prefix));
		let end = match prefix.iter().rposition(|&b| b != 0xff) {
			Some(i) => Bound::Excluded(EncodedKey::new(
				prefix.iter().take(i).copied().chain(iter::once(prefix[i] + 1)).collect::<Vec<_>>(),
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
				let mut prefixed = EncodedKey::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Included(prefixed)
			}
			Excluded(key) => {
				let mut prefixed = EncodedKey::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Excluded(prefixed)
			}
			Unbounded => Included(prefix.clone()),
		};

		let end = match self.end_bound() {
			Included(key) => {
				let mut prefixed = EncodedKey::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Included(prefixed)
			}
			Excluded(key) => {
				let mut prefixed = EncodedKey::with_capacity(prefix.len() + key.len());
				prefixed.extend_from_slice(prefix.as_ref());
				prefixed.extend_from_slice(key.as_ref());
				Excluded(prefixed)
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

	pub fn all() -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}

	pub fn parse(str: &str) -> Self {
		let (mut start, mut end) = (Bound::<EncodedKey>::Unbounded, Bound::<EncodedKey>::Unbounded);

		if let Some(dot_pos) = str.find("..") {
			let start_part = &str[..dot_pos];
			let end_part = &str[dot_pos + 2..];

			if !start_part.is_empty() {
				start = Bound::Included(EncodedKey::new(decode_binary(start_part)));
			}

			if let Some(end_str) = end_part.strip_prefix('=') {
				if !end_str.is_empty() {
					end = Bound::Included(EncodedKey::new(decode_binary(end_str)));
				}
			} else if !end_part.is_empty() {
				end = Bound::Excluded(EncodedKey::new(decode_binary(end_part)));
			}

			Self {
				start,
				end,
			}
		} else {
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
pub mod tests {
	use std::collections::Bound;

	use super::EncodedKey;

	macro_rules! as_key {
		($key:expr) => {{ EncodedKey::new(keycode::serialize(&$key)) }};
	}

	mod prefix {
		use std::ops::Bound;

		use crate::key::encoded::{
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

		use crate::key::{
			self as keycode,
			encoded::{EncodedKey, EncodedKeyRange, tests::included},
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

		use crate::key::encoded::EncodedKeyRange;

		#[test]
		fn test_is_unbounded() {
			let range = EncodedKeyRange::all();
			assert_eq!(range.start, Bound::Unbounded);
			assert_eq!(range.end, Bound::Unbounded);
		}
	}

	mod parse {
		use std::ops::Bound;

		use crate::key::encoded::{
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
		fn test_binary_encoded_row() {
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
