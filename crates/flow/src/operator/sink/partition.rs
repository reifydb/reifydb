// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::LazyLock};

use arrow_array::RecordBatch;
use postcard::to_stdvec;
use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{
	interface::catalog::{object::ObjectId, view::View},
	key::partition::PartitionKey,
	partition::{PartitionError, partition_of as core_partition_of},
};
use reifydb_value::{
	Result,
	value::{Value, blob::Blob, partition::Partition, value_type::ValueType},
};

use crate::{
	operator::sink::value_at,
	txn::{Batch, Get},
};

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

const VERIFIED_PARTITIONS_CAPACITY: usize = 65_536;

pub fn partition_of(
	view: &View,
	indices: &[usize],
	columns: &RecordBatch,
	row_idx: usize,
) -> Result<(Partition, Vec<Value>)> {
	let values: Vec<Value> = indices.iter().map(|&i| value_at(columns, i, row_idx)).collect::<Result<Vec<_>>>()?;
	Ok((core_partition_of(view.columns(), view.partition_by(), &values), values))
}

pub fn ensure_partition_unchanged(object: ObjectId, pre: Partition, post: Partition) -> Result<()> {
	if pre != post {
		return Err(PartitionError::ImmutablePartitionColumn {
			object,
		}
		.into());
	}
	Ok(())
}

pub fn resolve_partition_flow<T: Get + Batch + ?Sized>(
	txn: &mut T,
	object: ObjectId,
	partition: Partition,
	values: &[Value],
	verified: &mut HashMap<Partition, Vec<Value>>,
) -> Result<()> {
	if let Some(known) = verified.get(&partition) {
		if known.as_slice() != values {
			return Err(PartitionError::PartitionHashCollision {
				object,
				hash: partition.0,
			}
			.into());
		}
		return Ok(());
	}
	let key = PartitionKey::encoded(object, partition);
	let encoded = to_stdvec(values).expect("value postcard is total");
	let candidate = Value::Blob(Blob::from(encoded));
	match txn.get(&key)? {
		Some(row) => {
			if REGISTRY_SHAPE.get_value(&row, 0) != candidate {
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
			txn.set(&[key], &[row.freeze().into()])?;
		}
	}
	if verified.len() >= VERIFIED_PARTITIONS_CAPACITY {
		verified.clear();
	}
	verified.insert(partition, values.to_vec());
	Ok(())
}
