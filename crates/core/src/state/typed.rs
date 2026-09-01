// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_value::Result;

use crate::{
	key::{
		operator::{
			keyspace::columns_width,
			state::{GroupId, GroupStateKey, OperatorStateKey, keyspace_inner_range},
			traits::Keyspace,
		},
		typed::{
			Key,
			direction::Direction,
			layout::{KeyColumnType, KeyLayout, KeyValue},
		},
	},
	state::timer::StateStore,
};

pub trait SuffixBytes: Key {
	fn to_suffix_bytes(&self) -> Vec<u8>;

	fn from_suffix_bytes(bytes: &[u8]) -> Option<Self>
	where
		Self: Sized;
}

impl<T: KeyLayout> SuffixBytes for T {
	fn to_suffix_bytes(&self) -> Vec<u8> {
		let values = self.key_values();
		let mut out = Vec::with_capacity(columns_width(T::COLUMNS));
		for (value, column) in values.iter().zip(T::COLUMNS) {
			push_key_value(&mut out, *value, column.direction);
		}
		out
	}

	fn from_suffix_bytes(bytes: &[u8]) -> Option<Self> {
		let mut values = Vec::with_capacity(T::COLUMNS.len());
		let mut rest = bytes;
		for column in T::COLUMNS {
			let width = column.ty.width();
			if rest.len() < width {
				return None;
			}
			let (head, tail) = rest.split_at(width);
			values.push(key_value_from_bytes(column.ty, column.direction, head)?);
			rest = tail;
		}
		if !rest.is_empty() {
			return None;
		}
		T::from_key_values(&values)
	}
}

fn push_key_value(out: &mut Vec<u8>, value: KeyValue, direction: Direction) {
	let start = out.len();
	match value {
		KeyValue::U8(v) => out.push(v),
		KeyValue::U64(v) => out.extend_from_slice(&v.to_be_bytes()),
		KeyValue::Blob16(v) => out.extend_from_slice(&v),
	}
	if direction == Direction::Desc {
		for byte in &mut out[start..] {
			*byte = !*byte;
		}
	}
}

fn key_value_from_bytes(ty: KeyColumnType, direction: Direction, bytes: &[u8]) -> Option<KeyValue> {
	let flipped = direction == Direction::Desc;
	let at = |index: usize| {
		if flipped {
			!bytes[index]
		} else {
			bytes[index]
		}
	};
	match ty {
		KeyColumnType::U8 => (bytes.len() == 1).then(|| KeyValue::U8(at(0))),
		KeyColumnType::U64 => {
			let mut out = [0u8; 8];
			if bytes.len() != out.len() {
				return None;
			}
			for (index, byte) in out.iter_mut().enumerate() {
				*byte = at(index);
			}
			Some(KeyValue::U64(u64::from_be_bytes(out)))
		}
		KeyColumnType::Blob16 => {
			let mut out = [0u8; 16];
			if bytes.len() != out.len() {
				return None;
			}
			for (index, byte) in out.iter_mut().enumerate() {
				*byte = at(index);
			}
			Some(KeyValue::Blob16(out))
		}
	}
}

pub fn typed_key<K: Keyspace>(group: GroupId, suffix: &K::Suffix) -> GroupStateKey {
	OperatorStateKey::inner_encoded(group, K::ID, suffix.to_suffix_bytes())
}

pub trait TypedStateStore: StateStore {
	fn state_get_in<K>(&mut self, group: GroupId, suffix: &K::Suffix) -> Result<Option<EncodedPodRow>>
	where
		K: Keyspace;

	fn state_set_in<K>(&mut self, group: GroupId, suffix: &K::Suffix, row: EncodedPodRow) -> Result<()>
	where
		K: Keyspace;

	fn state_remove_in<K>(&mut self, group: GroupId, suffix: &K::Suffix) -> Result<()>
	where
		K: Keyspace;

	fn state_scan_in<K>(
		&mut self,
		group: GroupId,
		from: Bound<&K::Suffix>,
		limit: Option<usize>,
	) -> Result<Vec<(K::Suffix, EncodedPodRow)>>
	where
		K: Keyspace;
}

impl<T: StateStore + ?Sized> TypedStateStore for T {
	fn state_get_in<K>(&mut self, group: GroupId, suffix: &K::Suffix) -> Result<Option<EncodedPodRow>>
	where
		K: Keyspace,
	{
		self.state_get(&typed_key::<K>(group, suffix))
	}

	fn state_set_in<K>(&mut self, group: GroupId, suffix: &K::Suffix, row: EncodedPodRow) -> Result<()>
	where
		K: Keyspace,
	{
		self.state_set(&typed_key::<K>(group, suffix), row)
	}

	fn state_remove_in<K>(&mut self, group: GroupId, suffix: &K::Suffix) -> Result<()>
	where
		K: Keyspace,
	{
		self.state_remove(&typed_key::<K>(group, suffix))
	}

	fn state_scan_in<K>(
		&mut self,
		group: GroupId,
		from: Bound<&K::Suffix>,
		limit: Option<usize>,
	) -> Result<Vec<(K::Suffix, EncodedPodRow)>>
	where
		K: Keyspace,
	{
		let mut range = keyspace_inner_range(group, K::ID);
		range.start = match from {
			Bound::Unbounded => range.start,
			Bound::Included(suffix) => Bound::Included(typed_key::<K>(group, suffix).into_encoded()),
			Bound::Excluded(suffix) => Bound::Excluded(typed_key::<K>(group, suffix).into_encoded()),
		};
		let mut out = Vec::new();
		for (key, row) in self.state_page(range, limit)? {
			let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_slice())
				.expect("a group state key must decode as its own framing");
			let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix)
				.expect("a stored suffix must decode as the keyspace's own suffix type");
			out.push((suffix, row));
		}
		Ok(out)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::{encode_u64, encode_u64_asc, encode_u128_asc};
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

	use super::{SuffixBytes, typed_key};
	use crate::key::{
		operator::{
			keyspace::{
				expiry::{Expiry, ExpiryKey, TumblingExpiry, TumblingExpiryKey},
				join::{JoinLeft, JoinRight},
			},
			state::{GroupId, KeyspaceId, OperatorStateKey},
			traits::Keyspace,
		},
		typed::direction::{Asc, Desc},
	};

	#[test]
	fn a_multi_column_expiry_suffix_matches_the_bytes_the_index_is_written_with() {
		// expiry_due seeks by threshold alone into a key that also carries the owner, so if the typed
		// suffix laid its columns out differently the seek would land mid-key and skip live rows
		let threshold = 0x0102_0304_0506_0708u64;
		let owner = Hash128(0x1122_3344_5566_7788_99aa_bbcc_ddee_ff00);

		let typed = ExpiryKey {
			threshold: Desc(threshold),
			owner: Desc(owner),
		}
		.to_suffix_bytes();

		let mut expected = encode_u64(threshold).to_vec();
		expected.extend(owner.0.to_be_bytes().iter().map(|byte| !byte));
		assert_eq!(typed, expected);
	}

	#[test]
	fn a_descending_column_sorts_larger_values_first_once_it_reaches_the_wire() {
		// the expiry index is read newest-due-first, which is only true if Desc actually complements:
		// storing the plain value would drain the backlog in exactly the wrong order
		let small = ExpiryKey {
			threshold: Desc(1),
			owner: Desc(Hash128(0)),
		}
		.to_suffix_bytes();
		let large = ExpiryKey {
			threshold: Desc(u64::MAX),
			owner: Desc(Hash128(0)),
		}
		.to_suffix_bytes();

		assert!(large < small, "the larger threshold must sort first on the wire");
	}

	#[test]
	fn every_expiry_suffix_survives_the_round_trip_through_its_bytes() {
		// expire drops the rows it drained by rebuilding their keys from the scan, so a column that
		// decoded to a different value would leave the entry in the index and replay it forever
		for (threshold, window) in [(0u64, 0u64), (1, 2), (u64::MAX, u64::MAX), (7, u64::MAX)] {
			let key = TumblingExpiryKey {
				threshold: Desc(threshold),
				owner: Desc(Hash128(0xdead_beef)),
				window_start: Desc(window),
			};
			let bytes = key.to_suffix_bytes();
			assert_eq!(bytes.len(), 8 + 16 + 8, "threshold {threshold} window {window}");
			assert_eq!(TumblingExpiryKey::from_suffix_bytes(&bytes), Some(key));
		}
	}

	#[test]
	fn an_expiry_suffix_of_the_wrong_width_is_refused_rather_than_read_short() {
		// a truncated scan result must not decode into a key that addresses a live row
		let full = ExpiryKey {
			threshold: Desc(9),
			owner: Desc(Hash128(9)),
		}
		.to_suffix_bytes();

		assert_eq!(ExpiryKey::from_suffix_bytes(&full[..full.len() - 1]), None);
		assert_eq!(ExpiryKey::from_suffix_bytes(&[full.as_slice(), &[0u8]].concat()), None);
	}

	#[test]
	fn the_two_expiry_keyspaces_never_address_the_same_row() {
		// rolling and tumbling now hold separate ids; sharing one would let a rolling scan drain a
		// tumbling window's entry and lose it
		let rolling = typed_key::<Expiry>(
			GroupId::ROOT,
			&ExpiryKey {
				threshold: Desc(5),
				owner: Desc(Hash128(1)),
			},
		);
		let tumbling = typed_key::<TumblingExpiry>(
			GroupId::ROOT,
			&TumblingExpiryKey {
				threshold: Desc(5),
				owner: Desc(Hash128(1)),
				window_start: Desc(0),
			},
		);
		assert_ne!(rolling.as_slice(), tumbling.as_slice());
	}

	#[test]
	fn a_typed_key_is_byte_identical_to_the_hand_rolled_one_it_replaces() {
		// every ported call site keeps reading rows an unported one wrote, so a single byte of
		// difference here silently addresses a different row and both sides look healthy
		for row in [0u64, 1, 255, 256, u64::MAX] {
			let group = GroupId(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
			let typed = typed_key::<JoinLeft>(group, &Asc(RowNumber(row)));
			let legacy = OperatorStateKey::inner_encoded(group, KeyspaceId::JOIN_LEFT, encode_u64_asc(row));
			assert_eq!(typed.as_slice(), legacy.as_slice(), "row {row}");
		}
	}

	#[test]
	fn the_keyspace_id_separates_two_keyspaces_that_share_a_suffix_type() {
		// JOIN_LEFT and JOIN_RIGHT both key on Asc<RowNumber>; a key built without the keyspace byte
		// would let one side of a join read the other side's rows as its own
		let group = GroupId(7);
		let suffix = Asc(RowNumber(42));
		assert_ne!(
			typed_key::<JoinLeft>(group, &suffix).as_slice(),
			typed_key::<JoinRight>(group, &suffix).as_slice()
		);
	}

	#[test]
	fn the_group_leads_the_key_so_one_groups_rows_never_answer_for_another() {
		// the range scan for a group is a prefix scan on these bytes; a group that did not occupy the
		// leading sixteen bytes alone would make that prefix match rows from every other group
		let suffix = Asc(RowNumber(1));
		let one = typed_key::<JoinLeft>(GroupId(1), &suffix);
		let two = typed_key::<JoinLeft>(GroupId(2), &suffix);
		assert_ne!(&one.as_slice()[..16], &two.as_slice()[..16], "the group must vary in the leading bytes");
		assert_eq!(&one.as_slice()[16..], &two.as_slice()[16..], "nothing but the group may vary");
	}

	#[test]
	fn the_group_is_stored_descending_the_way_every_key_struct_declares_it() {
		// GroupId is Desc in all forty three key structs, and the byte layout complements it to match;
		// an ascending group here would order every prefix scan backwards against the typed Ord
		let suffix = Asc(RowNumber(1));
		let one = typed_key::<JoinLeft>(GroupId(1), &suffix);
		let two = typed_key::<JoinLeft>(GroupId(2), &suffix);
		assert!(one.as_slice() > two.as_slice(), "a larger group must sort earlier");
		assert_eq!(&one.as_slice()[..16], &encode_u128_asc(u128::MAX - 1)[..]);
	}

	#[test]
	fn a_suffix_survives_the_round_trip_for_every_scalar_width() {
		// state_group decodes the suffix back out of the stored key, so a width that does not round
		// trip turns a whole keyspace scan into a panic or, worse, a neighbouring key
		let row = Asc(RowNumber(0xdead_beef));
		assert_eq!(Asc::<RowNumber>::from_suffix_bytes(&row.to_suffix_bytes()), Some(row));

		let byte = Asc(9u8);
		assert_eq!(Asc::<u8>::from_suffix_bytes(&byte.to_suffix_bytes()), Some(byte));

		let blob = Asc([7u8; 16]);
		assert_eq!(<Asc<[u8; 16]>>::from_suffix_bytes(&blob.to_suffix_bytes()), Some(blob));

		assert_eq!(<()>::from_suffix_bytes(&().to_suffix_bytes()), Some(()));
	}

	#[test]
	fn a_suffix_of_the_wrong_width_is_refused_rather_than_padded() {
		// a short read means the stored key was written by a different layout; accepting it and padding
		// would decode as a valid but entirely different row number
		assert_eq!(Asc::<RowNumber>::from_suffix_bytes(&[0, 0, 0, 1]), None);
		assert_eq!(Asc::<RowNumber>::from_suffix_bytes(&[0; 9]), None);
		assert_eq!(Asc::<u8>::from_suffix_bytes(&[]), None);
		assert_eq!(<()>::from_suffix_bytes(&[0]), None);
	}

	#[test]
	fn the_suffix_encoding_preserves_the_ascending_order_of_its_values() {
		// the byte store serves a group by prefix scan in memcmp order, and the operator expects the
		// key type's own order; a little endian or variable width suffix would reverse or interleave it
		let mut encoded: Vec<Vec<u8>> = [0u64, 1, 255, 256, u64::MAX - 1, u64::MAX]
			.into_iter()
			.map(|row| Asc(RowNumber(row)).to_suffix_bytes())
			.collect();
		let sorted = {
			let mut copy = encoded.clone();
			copy.sort();
			copy
		};
		assert_eq!(encoded, sorted);
		encoded.dedup();
		assert_eq!(encoded.len(), 6, "distinct rows must encode distinctly");
	}

	#[test]
	fn a_keyspace_addresses_its_own_id_and_not_a_neighbouring_one() {
		// the keyspace byte sits between the group and the suffix; an off by one there points the read
		// at whatever keyspace happens to hold the next id
		let key = typed_key::<JoinLeft>(GroupId(3), &Asc(RowNumber(5)));
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice()).unwrap();
		assert_eq!(group, GroupId(3));
		assert_eq!(keyspace, JoinLeft::ID);
		assert_eq!(suffix, encode_u64_asc(5).to_vec());
	}
}
