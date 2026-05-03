// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::shape::ShapeId, internal, row::Ttl};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	#[allow(dead_code)]
	pub fn get_row_ttl(rx: &mut Transaction<'_>, shape: ShapeId) -> Result<Ttl> {
		Self::find_row_ttl(rx, shape)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"TTL config for shape {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				shape
			)))
		})
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
	fn test_get_row_ttl_existing() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));
		let config = Ttl {
			duration_nanos: 300_000_000_000,
			anchor: TtlAnchor::Created,
			cleanup_mode: TtlCleanupMode::Drop,
		};

		create_row_ttl(&mut txn, shape, &config).unwrap();

		let found = CatalogStore::get_row_ttl(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, config);
	}

	#[test]
	fn test_get_row_ttl_not_found_returns_error() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(999));

		let err = CatalogStore::get_row_ttl(&mut Transaction::Admin(&mut txn), shape).unwrap_err();
		assert!(err.diagnostic().message.contains("TTL config for shape"));
	}
}
