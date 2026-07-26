// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::LazyLock};

use postcard::to_stdvec;
use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
use reifydb_core::{
	interface::catalog::object::ObjectId, key::partition::PartitionKey, value::column::columns::Columns,
};
use reifydb_engine::error::EngineError;
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	value::{Value, blob::Blob, partition::Partition, value_type::ValueType},
};

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

pub(crate) fn partition_of(indices: &[usize], columns: &Columns, row_idx: usize) -> (Partition, Vec<Value>) {
	let values: Vec<Value> = indices.iter().map(|&i| columns.data_at(i).get_value(row_idx)).collect();
	(Partition::of(&values), values)
}

pub(crate) fn ensure_partition_unchanged(object: ObjectId, pre: Partition, post: Partition) -> Result<()> {
	if pre != post {
		return Err(EngineError::ImmutablePartitionColumn {
			object,
		}
		.into());
	}
	Ok(())
}

const VERIFIED_PARTITIONS_CAPACITY: usize = 65_536;

pub(crate) fn resolve_partition_flow(
	txn: &mut FlowTransaction,
	object: ObjectId,
	partition: Partition,
	values: &[Value],
	verified: &mut HashMap<Partition, Vec<Value>>,
) -> Result<()> {
	if let Some(known) = verified.get(&partition) {
		if known.as_slice() != values {
			return Err(EngineError::PartitionHashCollision {
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
				return Err(EngineError::PartitionHashCollision {
					object,
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
	if verified.len() >= VERIFIED_PARTITIONS_CAPACITY {
		verified.clear();
	}
	verified.insert(partition, values.to_vec());
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::TableId;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;

	use super::*;

	fn txn() -> FlowTransaction {
		let engine = TestEngine::new();
		engine.flow_txn().clock_millis(0).deferred()
	}

	// The verified map now lives on the sink operator, so the same partition arriving in every
	// apply (the steady state for a partitioned ring buffer: one registry point get per apply,
	// ~11M per profiling window) must resolve from memory. The first resolution registers and
	// reads; the repeat must not touch the store at all.
	#[test]
	fn a_verified_partition_never_rereads_the_store() {
		let mut txn = txn();
		let mut verified: HashMap<Partition, Vec<Value>> = HashMap::new();
		let object = ObjectId::table(TableId(1));
		let values = vec![Value::Utf8("sol".to_string())];
		let partition = Partition::of(&values);

		resolve_partition_flow(&mut txn, object, partition, &values, &mut verified).unwrap();
		let reads_after_first = txn.store_reads();
		assert!(reads_after_first > 0, "the first resolution must verify against the store");

		resolve_partition_flow(&mut txn, object, partition, &values, &mut verified).unwrap();
		assert_eq!(
			txn.store_reads(),
			reads_after_first,
			"a partition verified within this operator's lifetime must resolve from memory"
		);
	}

	// Hoisting the map across applies must not silently drop the hash-collision guard the
	// per-apply store read used to provide: different values under an already-verified partition
	// hash are corruption and must fail loudly. (A genuine 128-bit collision cannot be
	// constructed, so the mismatched pair is passed in directly, exactly as a collision would
	// arrive from partition_of.)
	#[test]
	fn a_verified_partition_still_detects_hash_collisions() {
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
