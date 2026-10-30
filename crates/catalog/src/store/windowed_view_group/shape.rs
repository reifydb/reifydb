// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_allocvec};
use reifydb_core::interface::catalog::id::ViewId;
use reifydb_macro::catalog_shape;
use reifydb_value::value::blob::Blob;

catalog_shape! {
	pub(crate) windowed_view_group {
		id: u64,
		namespace: u64,
		name: utf8,
		flow: u64,
		members: blob,
	}

	pub(crate) windowed_view_group_namespace {
		id: u64,
		name: utf8,
	}
}

pub(crate) fn serialize_members(members: &[ViewId]) -> Blob {
	let ids: Vec<u64> = members.iter().map(|member| member.0).collect();
	Blob::from(to_allocvec(&ids).expect("postcard serialization of a u64 list is total"))
}

pub(crate) fn deserialize_members(blob: &Blob) -> Vec<ViewId> {
	let ids: Vec<u64> = from_bytes(blob.as_bytes()).expect("stored windowed view group members must decode");
	ids.into_iter().map(ViewId).collect()
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::ViewId;

	use super::{deserialize_members, serialize_members};

	#[test]
	fn test_members_round_trip_in_declaration_order() {
		// Members must keep declaration order; a sorting or deduplicating encoding would break that.
		let original = vec![ViewId(30), ViewId(10), ViewId(20)];
		assert_eq!(deserialize_members(&serialize_members(&original)), original);
	}

	#[test]
	fn test_empty_members_round_trip() {
		assert!(deserialize_members(&serialize_members(&[])).is_empty());
	}
}
