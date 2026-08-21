// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::LazyLock};

use postcard::to_stdvec;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::{
	interface::catalog::{id::TableId, object::ObjectId, table::Table},
	key::{partition::PartitionKey, partitioned_row::PartitionedRowKey, row::RowKey},
	partition::{PartitionError, partition_col_indices},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, blob::Blob, partition::Partition, row_number::RowNumber, value_type::ValueType};

use crate::Result;

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

pub fn partition_values(shape: &RowShape, row: &[u8], indices: &[usize]) -> Vec<Value> {
	indices.iter().map(|&i| shape.get_value(row, i)).collect()
}

pub fn table_partition_of_row(table: &Table, shape: &RowShape, row: &[u8]) -> Partition {
	let indices = partition_col_indices(&table.columns, &table.partition_by);
	Partition::of(&partition_values(shape, row, &indices))
}

pub fn table_row_key(table: &Table, shape: &RowShape, row: &[u8], row_number: RowNumber) -> EncodedKey {
	if table.partition_by.is_empty() {
		RowKey::encoded(table.id, row_number)
	} else {
		let partition = table_partition_of_row(table, shape, row);
		PartitionedRowKey::encoded(table.id, partition, row_number)
	}
}

pub fn row_key_from_partition(table_id: TableId, partition: Option<Partition>, row_number: RowNumber) -> EncodedKey {
	match partition {
		None => RowKey::encoded(table_id, row_number),
		Some(partition) => PartitionedRowKey::encoded(table_id, partition, row_number),
	}
}

pub fn resolve_partition(
	txn: &mut Transaction<'_>,
	object: ObjectId,
	partition: Partition,
	values: &[Value],
	verified: &mut HashSet<Partition>,
) -> Result<()> {
	if !verified.insert(partition) {
		return Ok(());
	}
	let key = PartitionKey::encoded(object, partition);
	let encoded = to_stdvec(values).expect("value postcard is total");
	let candidate = Value::Blob(Blob::from(encoded));
	match txn.get(&key)? {
		Some(multi) => {
			if REGISTRY_SHAPE.get_value(&multi.bytes, 0) != candidate {
				return Err(PartitionError::PartitionHashCollision {
					object,
					hash: partition.0,
				}
				.into());
			}
		}
		None => {
			let mut row = REGISTRY_SHAPE.allocate_pod();
			REGISTRY_SHAPE.set_value(&mut row, 0, &candidate);
			txn.set(&key, row.freeze())?;
		}
	}
	Ok(())
}
