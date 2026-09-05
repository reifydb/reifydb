// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Ordering;

use smallvec::SmallVec;

use crate::key::{
	any::{AnyKey, Field, KeyFields},
	kind::KeyKind,
};

pub type OwnedField = Field<'static>;

#[derive(Debug, Clone)]
pub enum AnyKeyBound {
	Kind(KeyKind),
	KindEnd(KeyKind),
	Prefix(KeyKind, SmallVec<[OwnedField; 6]>),
	Key(AnyKey),
}

impl AnyKeyBound {
	pub fn prefix(kind: KeyKind, fields: impl IntoIterator<Item = OwnedField>) -> Self {
		Self::Prefix(kind, fields.into_iter().collect())
	}

	fn kind_byte(&self) -> u8 {
		match self {
			Self::Kind(kind) | Self::Prefix(kind, _) => *kind as u8,
			Self::KindEnd(kind) => (*kind as u8).wrapping_sub(1),
			Self::Key(key) => key.kind() as u8,
		}
	}

	fn bound_fields(&self) -> SmallVec<[Field<'_>; 6]> {
		match self {
			Self::Kind(_) | Self::KindEnd(_) => SmallVec::new(),
			Self::Prefix(_, fields) => fields.iter().cloned().collect(),
			Self::Key(key) => key.fields(),
		}
	}
}

impl Ord for AnyKeyBound {
	fn cmp(&self, other: &Self) -> Ordering {
		other.kind_byte().cmp(&self.kind_byte()).then_with(|| self.bound_fields().cmp(&other.bound_fields()))
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

impl From<AnyKey> for AnyKeyBound {
	fn from(key: AnyKey) -> Self {
		Self::Key(key)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_value::value::row_number::RowNumber;

	use super::{AnyKeyBound, OwnedField};
	use crate::{
		interface::catalog::{id::TableId, object::ObjectId, storage::StorageId},
		key::{
			any::{AnyKey, Field},
			catalog::TableKey,
			kind::KeyKind,
			row::RowKey,
			typed::key::Key,
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
				let encoded = Key::encode(&key);
				out.push((AnyKey::from(key), encoded));
			}
		}
		for table in [1u64, 2] {
			let key = TableKey {
				table: TableId(table),
			};
			let encoded = Key::encode(&key);
			out.push((AnyKey::from(key), encoded));
		}
		out
	}

	fn storage_start(storage: StorageId) -> AnyKeyBound {
		AnyKeyBound::prefix(
			KeyKind::Row,
			[
				OwnedField::UAsc(ObjectId::from(storage).type_tag() as u128),
				Field::UDesc(ObjectId::from(storage).as_u64() as u128),
			],
		)
	}

	fn storage_end(storage: StorageId) -> AnyKeyBound {
		let previous = ObjectId::from(storage).prev();
		AnyKeyBound::prefix(
			KeyKind::Row,
			[OwnedField::UAsc(previous.type_tag() as u128), Field::UDesc(previous.as_u64() as u128)],
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
