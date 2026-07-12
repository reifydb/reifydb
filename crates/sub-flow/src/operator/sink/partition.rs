// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::LazyLock};

use postcard::to_stdvec;
use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
use reifydb_core::{interface::catalog::shape::ShapeId, key::partition::PartitionKey, value::column::columns::Columns};
use reifydb_engine::error::EngineError;
use reifydb_value::{
	Result,
	value::{Value, blob::Blob, partition::Partition, value_type::ValueType},
};

use crate::transaction::FlowTransaction;

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

pub(crate) fn partition_of(indices: &[usize], columns: &Columns, row_idx: usize) -> (Partition, Vec<Value>) {
	let values: Vec<Value> = indices.iter().map(|&i| columns.data_at(i).get_value(row_idx)).collect();
	(Partition::of(&values), values)
}

pub(crate) fn ensure_partition_unchanged(shape: ShapeId, pre: Partition, post: Partition) -> Result<()> {
	if pre != post {
		return Err(EngineError::ImmutablePartitionColumn {
			shape,
		}
		.into());
	}
	Ok(())
}

pub(crate) fn resolve_partition_flow(
	txn: &mut FlowTransaction,
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
		Some(row) => {
			if REGISTRY_SHAPE.get_value(&row, 0) != candidate {
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
