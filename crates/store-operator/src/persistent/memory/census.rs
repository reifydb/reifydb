// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Borrow, collections::BTreeMap};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_codec::key::encoded::EncodedKey;
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
	fn state_sizes<Q: Borrow<GroupStateKey>>(
		&self,
		operator: OperatorId,
		keys: &[Q],
	) -> Result<Vec<Option<ByteSize>>> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Ok(vec![None; keys.len()]);
		};
		Ok(keys.iter()
			.map(|key| rows.get(key.borrow()).map(|row| ByteSize::from_bytes(row.len() as u64)))
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

impl MemoryPersistent {
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	#[instrument(name = "store::operator::persistent::memory::state_keys_after", level = "trace", skip_all)]
	pub fn state_keys_after(
		&self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		after: Option<&EncodedKey>,
		limit: u64,
	) -> Vec<EncodedKey> {
		let rows = self.0.rows.lock();
		let Some(rows) = rows.get(&operator) else {
			return Vec::new();
		};
		rows.keys()
			.filter(|key| key.keyspace() == Some(keyspace))
			.filter(|key| after.is_none_or(|after| key.as_encoded() > after))
			.take(usize::try_from(limit).unwrap_or(usize::MAX))
			.map(|key| key.as_encoded().clone())
			.collect()
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
