// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		change::{CatalogTrackOperatorTtlChangeOperations, CatalogTrackRowTtlChangeOperations},
		flow::FlowNodeId,
		shape::ShapeId,
	},
	key::{operator_ttl::OperatorTtlKey, row_ttl::RowTtlKey},
	row::Ttl,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use super::encode_ttl_config;
use crate::Result;

pub fn create_row_ttl(txn: &mut AdminTransaction, shape: ShapeId, config: &Ttl) -> Result<()> {
	let value = encode_ttl_config(config);
	txn.set(&RowTtlKey::encoded(shape), value)?;
	txn.track_row_ttl_created(shape, config.clone())?;
	Ok(())
}

pub fn create_operator_ttl(txn: &mut AdminTransaction, node: FlowNodeId, config: &Ttl) -> Result<()> {
	let value = encode_ttl_config(config);
	txn.set(&OperatorTtlKey::encoded(node), value)?;
	txn.track_operator_ttl_created(node, config.clone())?;
	Ok(())
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, SeriesId, TableId},
		row::{Ttl, TtlAnchor, TtlCleanupMode},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::CatalogStore;

	#[test]
	fn test_create_row_ttl_for_table() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));
		let config = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		create_row_ttl(&mut txn, shape, &config).unwrap();

		let found = CatalogStore::find_row_ttl(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("TTL config should be stored");
		assert_eq!(found, config);
	}

	#[test]
	fn test_create_row_ttl_for_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::RingBuffer(RingBufferId(200));
		let config = Ttl {
			duration_nanos: 3_600_000_000_000,
			anchor: TtlAnchor::Updated,
			cleanup_mode: TtlCleanupMode::Delete,
		};

		create_row_ttl(&mut txn, shape, &config).unwrap();

		let found = CatalogStore::find_row_ttl(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("TTL config should be stored");
		assert_eq!(found, config);
	}

	#[test]
	fn test_create_row_ttl_for_series() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Series(SeriesId(7));
		let config = Ttl {
			duration_nanos: 86_400_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		create_row_ttl(&mut txn, shape, &config).unwrap();

		let found = CatalogStore::find_row_ttl(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("TTL config should be stored");
		assert_eq!(found, config);
	}

	#[test]
	fn test_create_row_ttl_overwrite() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));
		let config_v1 = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};
		let config_v2 = Ttl {
			duration_nanos: 600_000_000_000,
			anchor: TtlAnchor::Updated,
			cleanup_mode: TtlCleanupMode::Delete,
		};

		create_row_ttl(&mut txn, shape, &config_v1).unwrap();
		create_row_ttl(&mut txn, shape, &config_v2).unwrap();

		let found = CatalogStore::find_row_ttl(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("TTL config should be stored");
		assert_eq!(found, config_v2);
	}
}
