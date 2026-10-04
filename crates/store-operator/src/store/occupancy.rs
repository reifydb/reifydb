// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{KeyspaceId, KeyspaceMask, OperatorStateKey},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::reifydb_assertions;
use tracing::instrument;

use crate::{error::Result, types::OperatorWrite};

#[derive(Debug, Default, Clone, Copy)]
struct Occupancy {
	mask: KeyspaceMask,
	seeded: bool,
}

#[derive(Debug, Default)]
pub struct KeyspaceOccupancy {
	masks: Mutex<HashMap<OperatorId, Occupancy>>,
}

impl KeyspaceOccupancy {
	pub fn new() -> Self {
		reifydb_assertions! {
			for spec in reifydb_core::key::operator::keyspace::KEYSPACES {
				assert!(
					KeyspaceMask::of([spec.id]).holds(spec.id),
					"store::operator::occupancy keyspace {} has no occupancy bit",
					spec.id.0
				);
			}
		}
		Self::default()
	}

	#[instrument(name = "store::operator::occupancy::record", level = "debug", skip_all, fields(write_count = writes.len()))]
	pub fn record(&self, writes: &[OperatorWrite]) {
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
			masks.entry(operator).or_default().mask.insert(keyspace);
		}
	}

	pub fn mask(
		&self,
		operator: OperatorId,
		seed: impl FnOnce() -> Result<Vec<KeyspaceId>>,
	) -> Result<KeyspaceMask> {
		if let Some(entry) = self.masks.lock().get(&operator)
			&& entry.seeded
		{
			return Ok(entry.mask);
		}
		let seeded = seed()?;
		let mut masks = self.masks.lock();
		let entry = masks.entry(operator).or_default();
		for keyspace in seeded {
			entry.mask.insert(keyspace);
		}
		entry.seeded = true;
		Ok(entry.mask)
	}

	pub fn occupied(&self, operator: OperatorId) -> Vec<KeyspaceId> {
		self.masks.lock().get(&operator).map(|entry| entry.mask).unwrap_or_default().held()
	}

	pub fn forget(&self, operator: OperatorId) {
		self.masks.lock().remove(&operator);
	}
}
