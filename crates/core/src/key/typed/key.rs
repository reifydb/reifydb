// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;

use crate::key::kind::KeyKind;

pub trait Key: Sized {
	const KIND: KeyKind;

	fn encode(&self) -> EncodedKey;

	fn decode(key: &EncodedKey) -> Option<Self>;
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_macro::Key;
	use reifydb_value::value::row_number::RowNumber;

	use super::Key;
	use crate::key::{kind::KeyKind, operator::state::GroupId};

	#[derive(Debug, Clone, PartialEq, Key)]
	#[key(kind = Row)]
	struct ProbeRowKey {
		table: u64,
		row: RowNumber,
	}

	#[derive(Debug, Clone, PartialEq, Key)]
	#[key(kind = Index)]
	struct ProbeGroupKey {
		group: GroupId,
		slot: [u8; 16],
	}

	#[derive(Debug, Clone, PartialEq, Key)]
	#[key(kind = Table)]
	struct ProbeNarrowKey {
		tag: u8,
	}

	#[test]
	fn encode_starts_with_the_kind_byte() {
		let key = ProbeRowKey {
			table: 7,
			row: RowNumber(1),
		};
		// extend_u8 inverts bits, matching the kind byte's inversion in every hand-rolled EncodableKey impl
		assert_eq!(key.encode().as_slice()[0], !(KeyKind::Row as u8));
	}

	#[test]
	fn encode_lays_out_fields_in_declaration_order() {
		// extend_u64 inverts bits so byte order sorts descending, matching the codebase's default
		let key = ProbeRowKey {
			table: 0x0102030405060708,
			row: RowNumber(0x0910111213141516),
		};
		let encoded = key.encode();
		let expected: Vec<u8> = std::iter::once(!(KeyKind::Row as u8))
			.chain((!0x0102030405060708u64).to_be_bytes())
			.chain((!0x0910111213141516u64).to_be_bytes())
			.collect();
		assert_eq!(encoded.as_slice(), expected.as_slice());
	}

	#[test]
	fn decode_recovers_the_original_struct() {
		let key = ProbeRowKey {
			table: 42,
			row: RowNumber(9),
		};
		let encoded = key.encode();
		assert_eq!(ProbeRowKey::decode(&encoded), Some(key));
	}

	#[test]
	fn decode_refuses_a_key_of_the_wrong_kind() {
		let foreign = ProbeNarrowKey {
			tag: 3,
		}
		.encode();
		assert_eq!(ProbeRowKey::decode(&foreign), None);
	}

	#[test]
	fn decode_refuses_truncated_bytes() {
		let key = ProbeRowKey {
			table: 1,
			row: RowNumber(1),
		};
		let mut bytes = key.encode().as_slice().to_vec();
		bytes.truncate(bytes.len() - 1);
		assert_eq!(ProbeRowKey::decode(&EncodedKey::new(bytes)), None);
	}

	#[test]
	fn group_id_and_fixed_byte_array_fields_round_trip() {
		let key = ProbeGroupKey {
			group: GroupId(0x1122_3344_5566_7788_99aa_bbcc_ddee_ff00),
			slot: [7u8; 16],
		};
		let encoded = key.encode();
		assert_eq!(encoded.as_slice().len(), 1 + 16 + 16);
		assert_eq!(ProbeGroupKey::decode(&encoded), Some(key));
	}

	#[test]
	fn a_single_field_key_round_trips() {
		let key = ProbeNarrowKey {
			tag: 0xAB,
		};
		let encoded = key.encode();
		assert_eq!(encoded.as_slice(), &[!(KeyKind::Table as u8), !0xABu8]);
		assert_eq!(ProbeNarrowKey::decode(&encoded), Some(key));
	}

	#[test]
	fn a_larger_field_value_sorts_first_by_default() {
		// extend_u64 inverts bits, so a smaller field value must produce the larger byte string
		let smaller = ProbeRowKey {
			table: 1,
			row: RowNumber(0),
		}
		.encode();
		let larger = ProbeRowKey {
			table: 1,
			row: RowNumber(1),
		}
		.encode();
		assert!(smaller > larger);
	}
}
