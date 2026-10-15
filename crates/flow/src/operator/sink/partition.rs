// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::RecordBatch;
use reifydb_core::{
	interface::catalog::{object::ObjectId, view::View},
	key::partition::PartitionKey,
	partition::{BatchPartitions, PartitionError, partitions_of_batch, registry_matches, registry_row},
};
use reifydb_value::{
	Result, reifydb_assertions,
	value::{Value, column_view::ColumnView, partition::Partition},
};

use crate::txn::{Batch, Get};

const VERIFIED_PARTITIONS_CAPACITY: usize = 65_536;

pub fn ensure_partition_unchanged(object: ObjectId, pre: Partition, post: Partition) -> Result<()> {
	if pre != post {
		return Err(PartitionError::ImmutablePartitionColumn {
			object,
		}
		.into());
	}
	Ok(())
}

#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
pub fn partition_batch(view: &View, indices: &[usize], columns: &RecordBatch) -> Result<BatchPartitions> {
	let keys = indices
		.iter()
		.map(|&i| ColumnView::try_from((columns.column(i), columns.schema_ref().field(i))))
		.collect::<Result<Vec<_>>>()?;
	reifydb_assertions! {
		for (&i, key) in indices.iter().zip(&keys) {
			let column = &view.columns()[i];
			assert!(
				column.dictionary_id.is_none()
					|| key.base_type() == reifydb_value::value::value_type::ValueType::DictionaryId
					|| key.none_count() == key.len(),
				"partition column '{}' is dictionary-encoded but was hashed from {:?}: stored rows hash the \
				 entry id, so a plain value lands in a different partition",
				column.name,
				key.base_type()
			);
		}
	}
	Ok(partitions_of_batch(&keys))
}

pub fn resolve_partitions_flow<T: Get + Batch + ?Sized>(
	txn: &mut T,
	object: ObjectId,
	distinct: &[(Partition, Vec<Value>)],
	verified: &mut HashMap<Partition, Vec<Value>>,
) -> Result<()> {
	let collision = |partition: Partition| -> Result<()> {
		Err(PartitionError::PartitionHashCollision {
			object,
			hash: partition.0,
		}
		.into())
	};
	let mut fresh: HashMap<Partition, &[Value]> = HashMap::new();
	let mut keys = Vec::new();
	let mut rows = Vec::new();
	for (partition, values) in distinct {
		let known = verified.get(partition).map(Vec::as_slice).or_else(|| fresh.get(partition).copied());
		if let Some(known) = known {
			if known != values.as_slice() {
				return collision(*partition);
			}
			continue;
		}
		let key = PartitionKey::encoded(object, *partition);
		match txn.get(&key)? {
			Some(row) => {
				if !registry_matches(&row, values) {
					return collision(*partition);
				}
			}
			None => {
				keys.push(key);
				rows.push(registry_row(values));
			}
		}
		fresh.insert(*partition, values);
	}
	if !keys.is_empty() {
		txn.set(keys, rows)?;
	}
	for (partition, values) in fresh {
		if verified.len() >= VERIFIED_PARTITIONS_CAPACITY {
			verified.clear();
		}
		verified.insert(partition, values.to_vec());
	}
	Ok(())
}
