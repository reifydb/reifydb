// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::{operator_ttl::OperatorTtlKey, row_ttl::RowTtlKey},
	row::Ttl,
};
use reifydb_transaction::transaction::Transaction;

use super::decode_ttl_config;
use crate::{CatalogStore, Result};

impl CatalogStore {
	/// Find a TTL configuration for a shape
	/// Returns None if no TTL is configured
	#[allow(dead_code)]
	pub fn find_row_ttl(rx: &mut Transaction<'_>, shape: ShapeId) -> Result<Option<Ttl>> {
		let value = rx.get(&RowTtlKey::encoded(shape))?;
		Ok(value.and_then(|v| decode_ttl_config(&v.row)))
	}

	/// Find a per-operator TTL configuration for a flow node.
	#[allow(dead_code)]
	pub fn find_operator_ttl(rx: &mut Transaction<'_>, node: FlowNodeId) -> Result<Option<Ttl>> {
		let value = rx.get(&OperatorTtlKey::encoded(node))?;
		Ok(value.and_then(|v| decode_ttl_config(&v.row)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		row::{Ttl, TtlAnchor, TtlCleanupMode},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::ttl::create::create_row_ttl;

	#[test]
	fn test_find_row_ttl_existing() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));
		let config = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		create_row_ttl(&mut txn, shape, &config).unwrap();

		let found = CatalogStore::find_row_ttl(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, Some(config));
	}

	#[test]
	fn test_find_row_ttl_not_found() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(999));

		let found = CatalogStore::find_row_ttl(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, None);
	}
}
