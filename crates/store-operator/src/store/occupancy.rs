// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

#[cfg(reifydb_assertions)]
use reifydb_core::key::operator::keyspace::KEYSPACES;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{KeyspaceId, OperatorStateKey},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::reifydb_assertions;
use tracing::instrument;

use crate::types::OperatorWrite;

const LOWEST_META: u8 = KeyspaceId::GUEST_ROW_MAPPING.0;

#[derive(Debug, Default)]
pub(crate) struct KeyspaceOccupancy {
	masks: Mutex<HashMap<OperatorId, u64>>,
}

impl KeyspaceOccupancy {
	pub(crate) fn new() -> Self {
		reifydb_assertions! {
			for spec in KEYSPACES {
				assert!(
					bit(spec.id).is_some(),
					"store::operator::occupancy keyspace {} has no occupancy bit",
					spec.id.0
				);
			}
		}
		Self::default()
	}

	#[instrument(name = "store::operator::occupancy::record", level = "debug", skip_all, fields(write_count = writes.len()))]
	pub(crate) fn record(&self, writes: &[OperatorWrite]) {
		let mut masks = self.masks.lock();
		for write in writes {
			let (operator, key) = match write {
				OperatorWrite::Insert {
					operator,
					key,
					..
				}
				| OperatorWrite::Replace {
					operator,
					key,
					..
				}
				| OperatorWrite::Remove {
					operator,
					key,
					..
				} => (*operator, key),
			};
			let Some((_, keyspace, _)) = OperatorStateKey::decode_inner(key.as_slice()) else {
				continue;
			};
			let Some(bit) = bit(keyspace) else {
				continue;
			};
			*masks.entry(operator).or_default() |= bit;
		}
	}

	pub(crate) fn mask(&self, operator: OperatorId, seed: impl FnOnce() -> Vec<KeyspaceId>) -> u64 {
		if let Some(mask) = self.masks.lock().get(&operator) {
			return *mask;
		}
		let seeded = seed().into_iter().filter_map(bit).fold(0, |mask, bit| mask | bit);
		let mut masks = self.masks.lock();
		let mask = masks.entry(operator).or_default();
		*mask |= seeded;
		*mask
	}

	pub(crate) fn forget(&self, operator: OperatorId) {
		self.masks.lock().remove(&operator);
	}
}

pub(crate) fn occupies(mask: u64, keyspace: KeyspaceId) -> bool {
	match bit(keyspace) {
		Some(bit) => mask & bit != 0,
		None => true,
	}
}

fn bit(keyspace: KeyspaceId) -> Option<u64> {
	let index = match keyspace.0 {
		id if id <= KeyspaceId::HIGHEST_DATA => id as u32,
		id if id >= LOWEST_META => (KeyspaceId::HIGHEST_DATA as u32) + 1 + (id - LOWEST_META) as u32,
		_ => return None,
	};
	(index < u64::BITS).then(|| 1u64 << index)
}
