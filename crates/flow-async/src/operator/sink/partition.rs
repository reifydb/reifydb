// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::LazyLock};

use postcard::to_stdvec;
use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{
	interface::catalog::object::ObjectId, key::partition::PartitionKey, partition::PartitionError,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	value::{Value, blob::Blob, partition::Partition, value_type::ValueType},
};

use crate::transaction::FlowTransaction;

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

pub(crate) fn partition_of(indices: &[usize], columns: &Columns, row_idx: usize) -> (Partition, Vec<Value>) {
	let values: Vec<Value> = indices.iter().map(|&i| columns.data_at(i).get_value(row_idx)).collect();
	(Partition::of(&values), values)
}

pub(crate) fn ensure_partition_unchanged(object: ObjectId, pre: Partition, post: Partition) -> Result<()> {
	if pre != post {
		return Err(PartitionError::ImmutablePartitionColumn {
			object,
		}
		.into());
	}
	Ok(())
}

const VERIFIED_PARTITIONS_CAPACITY: usize = 65_536;

pub(crate) fn resolve_partition_flow<T: FlowTransaction>(
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
			txn.set(&key, row.freeze())?;
		}
	}
	if verified.len() >= VERIFIED_PARTITIONS_CAPACITY {
		verified.clear();
	}
	verified.insert(partition, values.to_vec());
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::TableId;
	use reifydb_test_harness::engine::TestEngine;

	use super::*;
	use crate::transaction::{deferred::DeferredTransaction, mock::FlowTxn};

	fn txn() -> DeferredTransaction {
		let engine = TestEngine::new();
		engine.flow_txn().clock_millis(0).deferred()
	}

	#[test]
	fn a_verified_partition_still_detects_hash_collisions() {
		// Caching across applies must not drop the collision guard: different values under an
		// already-verified partition hash are corruption and must fail loudly. A real 128-bit
		// collision cannot be constructed, so the mismatched pair is passed in directly.
		let mut txn = txn();
		let mut verified: HashMap<Partition, Vec<Value>> = HashMap::new();
		let object = ObjectId::table(TableId(1));
		let values = vec![Value::Utf8("sol".to_string())];
		let partition = Partition::of(&values);

		resolve_partition_flow(&mut txn, object, partition, &values, &mut verified).unwrap();

		let colliding = vec![Value::Utf8("usdc".to_string())];
		let err = resolve_partition_flow(&mut txn, object, partition, &colliding, &mut verified);
		assert!(err.is_err(), "different values under a verified partition hash must be a hard error");
	}
}
