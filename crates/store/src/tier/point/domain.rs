// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{KeyspaceId, OperatorStateKey},
};

use crate::tier::point::PointDomain;

#[derive(Clone, Copy, Debug)]
pub(super) struct TestDomain;

pub(super) fn keyspace_of(key: &EncodedKey) -> Option<KeyspaceId> {
	let bytes = key.as_slice();
	let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
	if bytes.len() <= offset {
		return None;
	}
	Some(KeyspaceId(encode_u8(bytes[offset])))
}

impl PointDomain for TestDomain {
	type Dimension = OperatorId;
	type Slot = KeyspaceId;
	type Row = EncodedPodRow;

	const SLOTS: usize = 256;

	const SCOPE: &'static str = "operator_point";

	fn slot(key: &EncodedKey) -> Option<usize> {
		keyspace_of(key).map(|keyspace| keyspace.0 as usize)
	}

	fn caches_points(slot: usize) -> bool {
		KeyspaceId(slot as u8).cache_tiers().caches_points()
	}

	fn slot_at(index: usize) -> Self::Slot {
		KeyspaceId(index as u8)
	}

	fn slot_name(slot: Self::Slot) -> Cow<'static, str> {
		slot.name()
	}
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ChainingDomain;

impl PointDomain for ChainingDomain {
	type Dimension = OperatorId;
	type Slot = KeyspaceId;
	type Row = EncodedPodRow;

	const SLOTS: usize = 256;

	const SCOPE: &'static str = "chaining_point";

	fn slot(key: &EncodedKey) -> Option<usize> {
		keyspace_of(key).map(|keyspace| keyspace.0 as usize)
	}

	fn caches_points(slot: usize) -> bool {
		KeyspaceId(slot as u8).cache_tiers().caches_points()
	}

	fn supersede(resident: &mut Self::Row, incoming: Self::Row) -> bool {
		if incoming.len() < resident.len() {
			return false;
		}
		let mut merged = incoming.body().to_vec();
		merged.extend_from_slice(resident.body());
		*resident = EncodedPodRow::new(&merged);
		true
	}

	fn slot_at(index: usize) -> Self::Slot {
		KeyspaceId(index as u8)
	}

	fn slot_name(slot: Self::Slot) -> Cow<'static, str> {
		slot.name()
	}
}
