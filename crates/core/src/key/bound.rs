// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, ops::Bound};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use smallvec::SmallVec;

use crate::{
	interface::catalog::object::ObjectId,
	key::{
		any::{AnyKey, Field, KeyFields, Width},
		kind::KeyKind,
	},
};

pub type OwnedField = Field<'static>;

pub fn object_fields(object: ObjectId) -> [OwnedField; 2] {
	[Field::UAsc(Width::U8, object.type_tag() as u128), Field::UDesc(Width::U64, object.as_u64() as u128)]
}

#[derive(Debug, Clone)]
pub enum AnyKeyBound {
	Kind(KeyKind),
	KindEnd(KeyKind),
	Prefix(KeyKind, SmallVec<[OwnedField; 6]>),
	PrefixEnd(KeyKind, SmallVec<[OwnedField; 6]>),
	Key(AnyKey),
}

impl AnyKeyBound {
	pub fn prefix(kind: KeyKind, fields: impl IntoIterator<Item = OwnedField>) -> Self {
		Self::Prefix(kind, fields.into_iter().collect())
	}

	pub fn prefix_end(kind: KeyKind, fields: impl IntoIterator<Item = OwnedField>) -> Self {
		Self::PrefixEnd(kind, fields.into_iter().collect())
	}

	fn kind_byte(&self) -> u8 {
		match self {
			Self::Kind(kind) | Self::Prefix(kind, _) | Self::PrefixEnd(kind, _) => *kind as u8,
			Self::KindEnd(kind) => (*kind as u8).wrapping_sub(1),
			Self::Key(key) => key.kind() as u8,
		}
	}

	fn sorts_after_its_extensions(&self) -> bool {
		matches!(self, Self::PrefixEnd(..))
	}

	pub fn encode(&self) -> EncodedKey {
		if let Self::Key(key) = self {
			return key.encode();
		}
		let mut out = vec![!self.kind_byte()];
		for field in self.bound_fields().iter() {
			field.encode(&mut out);
		}
		if self.sorts_after_its_extensions() {
			match out.iter().rposition(|byte| *byte != 0xff) {
				Some(last) => {
					out.truncate(last + 1);
					out[last] += 1;
				}
				None => out.clear(),
			}
		}
		EncodedKey::new(out)
	}

	fn bound_fields(&self) -> SmallVec<[Field<'_>; 6]> {
		match self {
			Self::Kind(_) | Self::KindEnd(_) => SmallVec::new(),
			Self::Prefix(_, fields) | Self::PrefixEnd(_, fields) => fields.iter().cloned().collect(),
			Self::Key(key) => key.fields(),
		}
	}

	fn compare_fields(&self, other: &Self) -> Ordering {
		let left = self.bound_fields();
		let right = other.bound_fields();
		for (index, (left_field, right_field)) in left.iter().zip(right.iter()).enumerate() {
			let ordering = left_field.cmp(right_field);
			if ordering == Ordering::Equal {
				continue;
			}

			if index + 1 == left.len()
				&& self.sorts_after_its_extensions()
				&& left_field.is_truncation_of(right_field)
			{
				return Ordering::Greater;
			}
			if index + 1 == right.len()
				&& other.sorts_after_its_extensions()
				&& right_field.is_truncation_of(left_field)
			{
				return Ordering::Less;
			}
			return ordering;
		}
		match (
			left.len().cmp(&right.len()),
			self.sorts_after_its_extensions(),
			other.sorts_after_its_extensions(),
		) {
			(Ordering::Less, true, _) | (Ordering::Equal, true, false) => Ordering::Greater,
			(Ordering::Greater, _, true) | (Ordering::Equal, false, true) => Ordering::Less,
			(ordering, _, _) => ordering,
		}
	}
}

impl Ord for AnyKeyBound {
	fn cmp(&self, other: &Self) -> Ordering {
		other.kind_byte().cmp(&self.kind_byte()).then_with(|| self.compare_fields(other))
	}
}

impl PartialOrd for AnyKeyBound {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl PartialEq for AnyKeyBound {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Eq for AnyKeyBound {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnyKeyBoundRange {
	pub start: Bound<AnyKeyBound>,
	pub end: Bound<AnyKeyBound>,
}

impl AnyKeyBoundRange {
	pub fn start_end(start: AnyKeyBound, end: AnyKeyBound) -> Self {
		Self {
			start: Bound::Included(start),
			end: Bound::Included(end),
		}
	}

	pub fn prefix(kind: KeyKind, fields: impl IntoIterator<Item = OwnedField> + Clone) -> Self {
		Self {
			start: Bound::Included(AnyKeyBound::prefix(kind, fields.clone())),
			end: Bound::Excluded(AnyKeyBound::prefix_end(kind, fields)),
		}
	}

	pub fn kind(kind: KeyKind) -> Self {
		Self::start_end(AnyKeyBound::Kind(kind), AnyKeyBound::KindEnd(kind))
	}

	pub fn resume_after(self, last: Option<&AnyKey>) -> Self {
		match last {
			Some(last) => Self {
				start: Bound::Excluded(AnyKeyBound::Key(last.clone())),
				end: self.end,
			},
			None => self,
		}
	}

	pub fn resume_before(self, last: Option<&AnyKey>) -> Self {
		match last {
			Some(last) => Self {
				start: self.start,
				end: Bound::Excluded(AnyKeyBound::Key(last.clone())),
			},
			None => self,
		}
	}

	pub fn empty(kind: KeyKind) -> Self {
		Self {
			start: Bound::Excluded(AnyKeyBound::Kind(kind)),
			end: Bound::Excluded(AnyKeyBound::Kind(kind)),
		}
	}

	pub fn all() -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}

	pub fn contains(&self, bound: &AnyKeyBound) -> bool {
		let after_start = match &self.start {
			Bound::Unbounded => true,
			Bound::Included(start) => bound >= start,
			Bound::Excluded(start) => bound > start,
		};
		let before_end = match &self.end {
			Bound::Unbounded => true,
			Bound::Included(end) => bound <= end,
			Bound::Excluded(end) => bound < end,
		};
		after_start && before_end
	}

	pub fn encode(&self) -> EncodedKeyRange {
		EncodedKeyRange::new(encode_bound(&self.start), encode_bound(&self.end))
	}
}

fn encode_bound(bound: &Bound<AnyKeyBound>) -> Bound<EncodedKey> {
	match bound {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(key) => Bound::Included(key.encode()),
		Bound::Excluded(key) => {
			let encoded = key.encode();
			if encoded.is_empty() {
				return Bound::Unbounded;
			}
			Bound::Excluded(encoded)
		}
	}
}

impl From<AnyKey> for AnyKeyBound {
	fn from(key: AnyKey) -> Self {
		Self::Key(key)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::key::{
		encoded::{EncodedKey, EncodedKeyRange},
		serializer::KeySerializer,
	};
	use reifydb_value::value::row_number::RowNumber;

	use super::{AnyKeyBound, AnyKeyBoundRange, OwnedField, object_fields};
	use crate::{
		interface::catalog::{id::TableId, object::ObjectId, storage::StorageId},
		key::{
			EncodableKey,
			any::{AnyKey, Field, Width},
			catalog::{DictionaryKey, KeySerializerCatalogExt, TableKey},
			kind::KeyKind,
			row::RowKey,
		},
	};

	fn rows() -> Vec<(AnyKey, EncodedKey)> {
		let mut out = Vec::new();
		for storage in [1u64, 2, 3] {
			for row in [1u64, 2, u64::MAX] {
				let key = RowKey {
					storage: StorageId::table(storage),
					row: RowNumber(row),
				};
				let encoded = EncodableKey::encode(&key);
				out.push((AnyKey::from(key), encoded));
			}
		}
		for table in [1u64, 2] {
			let key = TableKey {
				table: TableId(table),
			};
			let encoded = EncodableKey::encode(&key);
			out.push((AnyKey::from(key), encoded));
		}
		out
	}

	fn storage_start(storage: StorageId) -> AnyKeyBound {
		AnyKeyBound::prefix(
			KeyKind::Row,
			[
				OwnedField::UAsc(Width::U8, ObjectId::from(storage).type_tag() as u128),
				Field::UDesc(Width::U64, ObjectId::from(storage).as_u64() as u128),
			],
		)
	}

	fn storage_end(storage: StorageId) -> AnyKeyBound {
		let previous = ObjectId::from(storage).prev();
		AnyKeyBound::prefix(
			KeyKind::Row,
			[
				OwnedField::UAsc(Width::U8, previous.type_tag() as u128),
				Field::UDesc(Width::U64, previous.as_u64() as u128),
			],
		)
	}

	#[test]
	fn a_typed_key_bound_orders_exactly_like_its_encoding() {
		let probes = rows();
		for (left, left_bytes) in &probes {
			for (right, right_bytes) in &probes {
				assert_eq!(
					AnyKeyBound::Key(left.clone()).cmp(&AnyKeyBound::Key(right.clone())),
					left_bytes.cmp(right_bytes),
					"{left:?} vs {right:?}"
				);
			}
		}
	}

	#[test]
	fn a_storage_prefix_selects_the_same_rows_as_the_encoded_range() {
		for storage in [1u64, 2, 3] {
			let storage = StorageId::table(storage);
			let byte_start = RowKey::storage_start(storage);
			let byte_end = RowKey::storage_end(storage);
			let typed_start = storage_start(storage);
			let typed_end = storage_end(storage);

			let probes = rows();
			let by_bytes: Vec<&AnyKey> = probes
				.iter()
				.filter(|(_, bytes)| *bytes >= byte_start && *bytes <= byte_end)
				.map(|(key, _)| key)
				.collect();
			let by_typed: Vec<&AnyKey> = probes
				.iter()
				.filter(|(key, _)| {
					let bound = AnyKeyBound::Key((*key).clone());
					bound >= typed_start && bound <= typed_end
				})
				.map(|(key, _)| key)
				.collect();

			assert!(!by_bytes.is_empty(), "storage {storage:?} selected nothing by bytes");
			assert_eq!(by_bytes, by_typed, "storage {storage:?}");
		}
	}

	#[test]
	fn a_field_prefix_bound_encodes_to_the_bytes_its_byte_producer_writes() {
		// the bound has to be substitutable for the encoded range it replaces, and ordering
		// alone cannot show that: two bounds can bracket the same typed keys while writing
		// different bytes, which would silently change what the sqlite blob range selects.
		for storage in [1u64, 2, u64::MAX] {
			let storage = StorageId::table(storage);
			assert_eq!(storage_start(storage).encode(), RowKey::storage_start(storage), "{storage:?}");
			assert_eq!(storage_end(storage).encode(), RowKey::storage_end(storage), "{storage:?}");
		}
	}

	#[test]
	fn a_kind_span_bound_encodes_the_kind_byte_and_its_predecessor() {
		// built through the codec rather than through DictionaryKey::full_scan, which now
		// returns this very bound and would make the assertion compare a value with itself.
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(<DictionaryKey as EncodableKey>::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(<DictionaryKey as EncodableKey>::KIND as u8 - 1);

		assert_eq!(AnyKeyBound::Kind(KeyKind::Dictionary).encode(), start.to_encoded_key());
		assert_eq!(AnyKeyBound::KindEnd(KeyKind::Dictionary).encode(), end.to_encoded_key());
	}

	fn storage_fields(storage: StorageId) -> Vec<OwnedField> {
		object_fields(ObjectId::from(storage)).to_vec()
	}

	#[test]
	fn the_object_id_field_pair_encodes_to_what_extend_object_id_writes() {
		// twenty-one producers project an ObjectId through this helper rather than through the
		// derive, so it is the one field pair with no generated conformance test behind it.
		for storage in [0u64, 1, 255, u64::MAX] {
			let object = ObjectId::from(StorageId::table(storage));
			let mut replayed = Vec::new();
			for field in object_fields(object) {
				field.encode(&mut replayed);
			}
			let mut expected = KeySerializer::with_capacity(9);
			expected.extend_object_id(object);
			assert_eq!(replayed.as_slice(), expected.to_encoded_key().as_slice(), "{object:?}");
		}
	}

	#[test]
	fn a_field_prefix_range_encodes_to_the_span_the_byte_prefix_helper_computes() {
		// EncodedKeyRange::prefix ends on the byte successor of the prefix, not on a decremented
		// field, so PrefixEnd has to reproduce that successor exactly or the range either drops
		// the last keys of the prefix or reaches into the next one.
		for storage in [1u64, 2, 255, u64::MAX] {
			let storage = StorageId::table(storage);
			let typed = AnyKeyBoundRange::prefix(KeyKind::Row, storage_fields(storage));
			let bytes = EncodedKeyRange::prefix(RowKey::storage_start(storage).as_slice());
			let encoded = typed.encode();
			assert_eq!(encoded.start, bytes.start, "{storage:?} start");
			assert_eq!(encoded.end, bytes.end, "{storage:?} end");
		}
	}

	#[test]
	fn a_prefix_range_selects_the_same_keys_typed_as_it_does_encoded() {
		for storage in [1u64, 2, 3] {
			let storage = StorageId::table(storage);
			let typed = AnyKeyBoundRange::prefix(KeyKind::Row, storage_fields(storage));
			let bytes = EncodedKeyRange::prefix(RowKey::storage_start(storage).as_slice());
			let (Bound::Included(typed_start), Bound::Excluded(typed_end)) =
				(typed.start.clone(), typed.end.clone())
			else {
				panic!("a field prefix range is expected to be included-excluded");
			};

			let probes = rows();
			let by_bytes: Vec<&AnyKey> = probes
				.iter()
				.filter(|(_, encoded)| contains(&bytes, encoded))
				.map(|(key, _)| key)
				.collect();
			let by_typed: Vec<&AnyKey> = probes
				.iter()
				.filter(|(key, _)| {
					let bound = AnyKeyBound::Key((*key).clone());
					bound >= typed_start && bound < typed_end
				})
				.map(|(key, _)| key)
				.collect();

			assert!(!by_bytes.is_empty(), "storage {storage:?} selected nothing by bytes");
			assert_eq!(by_bytes, by_typed, "storage {storage:?}");
		}
	}

	fn contains(range: &EncodedKeyRange, key: &EncodedKey) -> bool {
		let after_start = match &range.start {
			Bound::Unbounded => true,
			Bound::Included(start) => key >= start,
			Bound::Excluded(start) => key > start,
		};
		let before_end = match &range.end {
			Bound::Unbounded => true,
			Bound::Included(end) => key <= end,
			Bound::Excluded(end) => key < end,
		};
		after_start && before_end
	}

	fn mixed_bounds() -> Vec<AnyKeyBound> {
		let mut out = vec![
			AnyKeyBound::Kind(KeyKind::Row),
			AnyKeyBound::KindEnd(KeyKind::Row),
			AnyKeyBound::Kind(KeyKind::Table),
			AnyKeyBound::KindEnd(KeyKind::Table),
		];
		for storage in [1u64, 2, 3] {
			let storage = StorageId::table(storage);
			out.push(AnyKeyBound::prefix(KeyKind::Row, storage_fields(storage)));
			out.push(AnyKeyBound::prefix_end(KeyKind::Row, storage_fields(storage)));
		}
		out.extend(rows().into_iter().map(|(key, _)| AnyKeyBound::Key(key)));
		out
	}

	#[test]
	fn ordering_over_every_bound_shape_is_a_total_order() {
		// BTreeMap compares in both directions, so an asymmetric arm silently corrupts lookup
		// rather than failing loudly. A prefix end is only reached from one side by the range
		// tests above, which cannot see that.
		let bounds = mixed_bounds();
		for left in &bounds {
			for right in &bounds {
				assert_eq!(
					left.cmp(right),
					right.cmp(left).reverse(),
					"antisymmetry broken\n  left  = {left:?}\n  right = {right:?}"
				);
			}
		}
		for left in &bounds {
			for middle in &bounds {
				for right in &bounds {
					if left <= middle && middle <= right {
						assert!(
							left <= right,
							"transitivity broken\n  {left:?}\n  {middle:?}\n  {right:?}"
						);
					}
				}
			}
		}
	}

	#[test]
	fn a_prefix_end_sorts_above_every_key_that_extends_its_prefix() {
		for storage in [1u64, 2, 3] {
			let storage = StorageId::table(storage);
			let end = AnyKeyBound::prefix_end(KeyKind::Row, storage_fields(storage));
			let start = AnyKeyBound::prefix(KeyKind::Row, storage_fields(storage));
			let mut extensions = 0;
			for (key, _) in rows() {
				let bound = AnyKeyBound::Key(key.clone());
				if bound >= start && bound < end {
					extensions += 1;
					assert!(end > bound, "{end:?} must sort above {bound:?}");
					assert!(bound < end, "{bound:?} must sort below {end:?}");
				}
			}
			assert!(extensions > 0, "storage {storage:?} has no extension to compare against");
		}
	}

	#[test]
	fn a_kind_range_brackets_the_whole_kind_inclusively() {
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(<DictionaryKey as EncodableKey>::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(<DictionaryKey as EncodableKey>::KIND as u8 - 1);

		let encoded = AnyKeyBoundRange::kind(KeyKind::Dictionary).encode();
		assert_eq!(encoded.start, Bound::Included(start.to_encoded_key()));
		assert_eq!(encoded.end, Bound::Included(end.to_encoded_key()));
	}

	#[test]
	fn a_kind_span_brackets_every_key_of_that_kind_and_nothing_else() {
		let start = AnyKeyBound::Kind(KeyKind::Row);
		let end = AnyKeyBound::KindEnd(KeyKind::Row);
		assert!(start < end, "the kind span must not be empty");
		for (key, _) in rows() {
			let bound = AnyKeyBound::Key(key.clone());
			let inside = bound >= start && bound <= end;
			assert_eq!(inside, key.kind() == KeyKind::Row, "{key:?}");
		}
	}
}

#[cfg(test)]
mod bound_order_matches_encoded_order {
	use std::borrow::Cow;

	use reifydb_codec::key::serializer::KeySerializer;
	use smallvec::smallvec;

	use super::*;
	use crate::{
		interface::catalog::{
			id::{IndexId, TableId},
			object::ObjectId,
		},
		key::{
			any::{ByteEncoding, RawEncoding},
			catalog::IndexEntryKey,
		},
		value::index::encoded::EncodedIndexKey,
	};

	fn table() -> ObjectId {
		ObjectId::Table(TableId(1))
	}

	fn index() -> IndexId {
		IndexId::primary(1u64)
	}

	// Index tails hold whatever the caller encoded, so a string tail arrives inverted and a
	// prefix of the plaintext is a prefix of the encoded tail only after the same inversion.
	fn entry(tail: &str) -> AnyKeyBound {
		let mut serializer = KeySerializer::new();
		serializer.extend_str(tail);
		AnyKeyBound::Key(
			IndexEntryKey::new(table(), index(), EncodedIndexKey::new(serializer.finish().as_slice()))
				.into(),
		)
	}

	fn tail_prefix(byte: u8) -> SmallVec<[OwnedField; 6]> {
		object_fields(table())
			.into_iter()
			.chain([
				Field::UAsc(Width::U8, 1),
				Field::UDesc(Width::U64, index().as_u64() as u128),
				Field::RawAsc(RawEncoding::Verbatim, Cow::Owned(vec![!byte])),
			])
			.collect()
	}

	fn probes() -> Vec<AnyKeyBound> {
		vec![
			AnyKeyBound::Kind(KeyKind::IndexEntry),
			AnyKeyBound::Prefix(KeyKind::IndexEntry, tail_prefix(b'a')),
			AnyKeyBound::PrefixEnd(KeyKind::IndexEntry, tail_prefix(b'a')),
			AnyKeyBound::Prefix(KeyKind::IndexEntry, tail_prefix(b'b')),
			AnyKeyBound::PrefixEnd(KeyKind::IndexEntry, tail_prefix(b'b')),
			entry("a"),
			entry("a1"),
			entry("a3"),
			entry("aa"),
			entry("az"),
			entry("b"),
			entry("b1"),
			entry("b2"),
			entry("c1"),
			AnyKeyBound::Prefix(
				KeyKind::IndexEntry,
				object_fields(table()).into_iter().collect::<SmallVec<[OwnedField; 6]>>(),
			),
			AnyKeyBound::PrefixEnd(
				KeyKind::IndexEntry,
				object_fields(table()).into_iter().collect::<SmallVec<[OwnedField; 6]>>(),
			),
			AnyKeyBound::Prefix(
				KeyKind::IndexEntry,
				smallvec![Field::BytesDesc(ByteEncoding::Fixed, Cow::Owned(vec![7, 7]))],
			),
		]
	}

	#[test]
	fn a_bound_orders_against_a_key_the_way_their_bytes_do() {
		// Keys are what a bound is ultimately compared against: the pending-writes index is keyed
		// by `Key` bounds and ranged by the others, and the storage engine merges the same span
		// on bytes. A disagreement here is a range that silently includes or drops a row.
		//
		// Two non-key bounds may legitimately encode to the same byte position and still order
		// strictly against each other (`PrefixEnd` of one group is `Prefix` of the next), which
		// only makes range merging more conservative, so those pairs are not compared here.
		let probes = probes();
		for left in &probes {
			for right in &probes {
				if !matches!(left, AnyKeyBound::Key(_)) && !matches!(right, AnyKeyBound::Key(_)) {
					continue;
				}
				let left_bytes = left.encode();
				let right_bytes = right.encode();
				assert_eq!(
					left.cmp(right),
					left_bytes.as_slice().cmp(right_bytes.as_slice()),
					"bound order disagrees with encoded order\n  a = {left:?}\n  b = \
					 {right:?}\n  a bytes = {:02x?}\n  b bytes = {:02x?}",
					left_bytes.as_slice(),
					right_bytes.as_slice()
				);
			}
		}
	}

	#[test]
	fn every_bound_pair_spans_the_same_keys_typed_as_it_does_encoded() {
		// Ordering between two non-key bounds may differ from their bytes without harm, but the
		// set of keys a range admits may not: that set is the range's meaning.
		let probes = probes();
		let keys: Vec<&AnyKeyBound> =
			probes.iter().filter(|bound| matches!(bound, AnyKeyBound::Key(_))).collect();

		for start in &probes {
			for end in &probes {
				let typed_start = Bound::Included(start.clone());
				let typed_end = Bound::Excluded(end.clone());
				let raw_start = Bound::Included(start.encode());
				let raw_end = Bound::Excluded(end.encode());

				for probe in &keys {
					let bytes = probe.encode();
					assert_eq!(
						contains(&typed_start, &typed_end, probe),
						contains_bytes(&raw_start, &raw_end, &bytes),
						"typed and encoded spans disagree\n  start = {start:?}\n  end \
						 = {end:?}\n  key = {probe:?}"
					);
				}
			}
		}
	}

	#[test]
	fn a_prefix_range_contains_exactly_the_keys_its_encoded_form_contains() {
		// The regression that motivated the truncation rule: `PrefixEnd` over a tail that is a
		// strict byte prefix of a key's tail used to sort below that key, so a prefix range
		// excluded every key it was built to cover.
		let range = IndexEntryKey::key_prefix_range(table(), index(), &[!b'a']);
		let encoded = range.encode();

		for tail in ["a", "a1", "a3", "aa", "az", "b", "b1", "c1"] {
			let bound = entry(tail);
			let AnyKeyBound::Key(key) = &bound else {
				unreachable!("entry builds a Key bound");
			};
			let bytes = key.encode();

			let typed = contains(&range.start, &range.end, &bound);
			let raw = contains_bytes(&encoded.start, &encoded.end, &bytes);

			assert_eq!(typed, raw, "typed and encoded containment disagree for tail {tail}");
			assert_eq!(typed, tail.starts_with('a'), "wrong containment verdict for tail {tail}");
		}
	}

	fn contains(start: &Bound<AnyKeyBound>, end: &Bound<AnyKeyBound>, probe: &AnyKeyBound) -> bool {
		let lower = match start {
			Bound::Included(bound) => probe >= bound,
			Bound::Excluded(bound) => probe > bound,
			Bound::Unbounded => true,
		};
		let upper = match end {
			Bound::Included(bound) => probe <= bound,
			Bound::Excluded(bound) => probe < bound,
			Bound::Unbounded => true,
		};
		lower && upper
	}

	fn contains_bytes(start: &Bound<EncodedKey>, end: &Bound<EncodedKey>, probe: &EncodedKey) -> bool {
		let lower = match start {
			Bound::Included(key) => probe >= key,
			Bound::Excluded(key) => probe > key,
			Bound::Unbounded => true,
		};
		let upper = match end {
			Bound::Included(key) => probe <= key,
			Bound::Excluded(key) => probe < key,
			Bound::Unbounded => true,
		};
		lower && upper
	}
}
