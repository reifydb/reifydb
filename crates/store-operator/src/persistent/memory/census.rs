// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupStateKey, KeyspaceId},
};
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	error::Result,
	persistent::{
		Enumerate, Measure,
		memory::{MemoryPersistent, row_bytes},
	},
	types::OperatorStateCensus,
};

impl Measure for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::state_sizes", level = "trace", skip_all)]
	fn state_sizes(
		&self,
		operator: OperatorId,
		keys: &[GroupStateKey],
	) -> Result<HashMap<GroupStateKey, ByteSize>> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(HashMap::new());
		};
		Ok(keys.iter()
			.filter_map(|key| {
				rows.get(key).map(|row| (key.clone(), ByteSize::from_bytes(row.len() as u64)))
			})
			.collect())
	}

	#[instrument(name = "store::operator::persistent::memory::bytes", level = "trace", skip_all)]
	fn bytes(&self, operator: OperatorId) -> Result<ByteSize> {
		let rows = self.0.rows.lock();
		let total = rows
			.get(&operator)
			.map(|rows| rows.iter().map(|(key, row)| row_bytes(key, row)).sum())
			.unwrap_or(0u64);
		Ok(ByteSize::from_bytes(total))
	}
}

impl Enumerate for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::census", level = "trace", skip_all)]
	fn census(&self) -> Result<Vec<OperatorStateCensus>> {
		let rows = self.0.rows.lock();
		let mut counted: BTreeMap<(OperatorId, u8), OperatorStateCensus> = BTreeMap::new();
		for (operator, rows) in rows.iter() {
			for (key, row) in rows.iter() {
				let Some(keyspace) = key.keyspace() else {
					continue;
				};
				let entry = counted.entry((*operator, keyspace.0)).or_insert(OperatorStateCensus {
					operator: *operator,
					keyspace,
					keys: 0,
					key_bytes: ByteSize::from_bytes(0),
					value_bytes: ByteSize::from_bytes(0),
				});
				entry.keys += 1;
				entry.key_bytes =
					ByteSize::from_bytes(entry.key_bytes.as_bytes() + key.as_slice().len() as u64);
				entry.value_bytes =
					ByteSize::from_bytes(entry.value_bytes.as_bytes() + row.len() as u64);
			}
		}
		Ok(counted.into_values().collect())
	}

	#[instrument(name = "store::operator::persistent::memory::operators", level = "trace", skip_all)]
	fn operators(&self) -> Result<Vec<OperatorId>> {
		Ok(self.0.rows.lock().keys().copied().collect())
	}

	#[instrument(name = "store::operator::persistent::memory::keyspaces", level = "trace", skip_all)]
	fn keyspaces(&self, operator: OperatorId) -> Result<Vec<KeyspaceId>> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(Vec::new());
		};
		let mut seen: Vec<KeyspaceId> = rows.keys().filter_map(|key| key.keyspace()).collect();
		seen.sort_unstable();
		seen.dedup();
		Ok(seen)
	}
}
