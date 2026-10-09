// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	interface::catalog::{id::TableId, object::ObjectId},
	key::{
		any::TaggedKey,
		partition::PartitionKey,
		row::{PartitionedRowKey, RowKey},
	},
	partition::{PartitionError, registry_matches, registry_row},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	value::{Value, partition::Partition, row_number::RowNumber},
};

use crate::Result;

pub fn row_key_from_partition(table_id: TableId, partition: Option<Partition>, row_number: RowNumber) -> TaggedKey {
	match partition {
		None => RowKey::new(table_id, row_number).into(),
		Some(partition) => PartitionedRowKey::new(table_id, partition, row_number).into(),
	}
}

pub fn resolve_partitions(
	txn: &mut Transaction<'_>,
	object: ObjectId,
	distinct: &[(Partition, Vec<Value>)],
	verified: &mut HashMap<Partition, Vec<Value>>,
) -> Result<()> {
	for (partition, values) in distinct {
		match verified.get(partition) {
			Some(known) if known != values => return Err(collision(object, *partition)),
			Some(_) => {}
			None => {
				check_or_register(txn, object, *partition, values)?;
				verified.insert(*partition, values.clone());
			}
		}
	}
	Ok(())
}

fn check_or_register(
	txn: &mut Transaction<'_>,
	object: ObjectId,
	partition: Partition,
	values: &[Value],
) -> Result<()> {
	let key = PartitionKey::new(object, partition);
	match txn.get(&key)? {
		Some(multi) if !registry_matches(&multi.bytes, values) => Err(collision(object, partition)),
		Some(_) => Ok(()),
		None => txn.set(&key, registry_row(values)),
	}
}

fn collision(object: ObjectId, partition: Partition) -> Error {
	PartitionError::PartitionHashCollision {
		object,
		hash: partition.0,
	}
	.into()
}
