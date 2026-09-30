// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
	use std::collections::HashMap;

	use reifydb_core::interface::catalog::{id::TableId, object::ObjectId};
	use reifydb_flow::operator::sink::partition::resolve_partition_flow;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::value::{Value, partition::Partition};

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
