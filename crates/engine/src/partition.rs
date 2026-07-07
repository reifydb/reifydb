// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::LazyLock};

use postcard::to_stdvec;
use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, RowShapeField},
	},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	interface::catalog::{column::Column, id::TableId, shape::ShapeId, table::Table},
	key::{
		partition::PartitionKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, blob::Blob, partition::Partition, row_number::RowNumber, value_type::ValueType};

use crate::{Result, error::EngineError};

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

pub fn partition_col_indices(columns: &[Column], partition_by: &[String]) -> Vec<usize> {
	partition_by
		.iter()
		.map(|pb| {
			columns.iter()
				.position(|c| c.name == *pb)
				.expect("partition column must exist (validated during planning)")
		})
		.collect()
}

pub fn partition_values(shape: &RowShape, row: &EncodedRow, indices: &[usize]) -> Vec<Value> {
	indices.iter().map(|&i| shape.get_value(row, i)).collect()
}

pub fn table_partition_of_row(table: &Table, shape: &RowShape, row: &EncodedRow) -> Partition {
	let indices = partition_col_indices(&table.columns, &table.partition_by);
	Partition::of(&partition_values(shape, row, &indices))
}

pub fn table_row_key(table: &Table, shape: &RowShape, row: &EncodedRow, row_number: RowNumber) -> EncodedKey {
	if table.partition_by.is_empty() {
		RowKey::encoded(table.id, row_number)
	} else {
		let partition = table_partition_of_row(table, shape, row);
		PartitionedRowKey::encoded(ShapeId::Table(table.id), partition, RowLocator::Row(row_number))
	}
}

pub fn row_key_from_partition(table_id: TableId, partition: Option<Partition>, row_number: RowNumber) -> EncodedKey {
	match partition {
		None => RowKey::encoded(table_id, row_number),
		Some(partition) => {
			PartitionedRowKey::encoded(ShapeId::Table(table_id), partition, RowLocator::Row(row_number))
		}
	}
}

pub fn resolve_partition(
	txn: &mut Transaction<'_>,
	shape: ShapeId,
	partition: Partition,
	values: &[Value],
	verified: &mut HashSet<Partition>,
) -> Result<()> {
	if !verified.insert(partition) {
		return Ok(());
	}
	let key = PartitionKey::encoded(shape, partition);
	let encoded = to_stdvec(values).expect("value postcard is total");
	let candidate = Value::Blob(Blob::from(encoded));
	match txn.get(&key)? {
		Some(multi) => {
			if REGISTRY_SHAPE.get_value(&multi.row, 0) != candidate {
				return Err(EngineError::PartitionHashCollision {
					shape,
					hash: partition.0,
				}
				.into());
			}
		}
		None => {
			let mut row = REGISTRY_SHAPE.allocate();
			REGISTRY_SHAPE.set_value(&mut row, 0, &candidate);
			txn.set(&key, row)?;
		}
	}
	Ok(())
}
