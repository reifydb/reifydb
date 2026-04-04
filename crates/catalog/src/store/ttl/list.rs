// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::shape::ShapeId,
	key::{
		EncodableKey,
		ttl::{RowTtlKey, RowTtlKeyRange},
	},
	row::RowTtl,
};
use reifydb_transaction::transaction::Transaction;

use super::decode_ttl_config;
use crate::{CatalogStore, Result};

/// A shape TTL configuration entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowTtlEntry {
	pub shape: ShapeId,
	pub config: RowTtl,
}

impl CatalogStore {
	/// List all TTL configurations for shapes
	#[allow(dead_code)]
	pub fn list_row_ttls(rx: &mut Transaction<'_>) -> Result<Vec<RowTtlEntry>> {
		let mut result = Vec::new();

		let stream = rx.range(RowTtlKeyRange::full_scan(), 1024)?;

		for entry in stream {
			let entry = entry?;
			if let Some(key) = RowTtlKey::decode(&entry.key)
				&& let Some(config) = decode_ttl_config(&entry.row)
			{
				result.push(RowTtlEntry {
					shape: key.shape,
					config,
				});
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, SeriesId, TableId},
		row::{RowTtl, RowTtlAnchor, RowTtlCleanupMode},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::ttl::create::create_row_ttl;

	#[test]
	fn test_list_row_ttls_empty() {
		let mut txn = create_test_admin_transaction();

		let entries = CatalogStore::list_row_ttls(&mut Transaction::Admin(&mut txn)).unwrap();
		assert!(entries.is_empty());
	}

	#[test]
	fn test_list_row_ttls_multiple() {
		let mut txn = create_test_admin_transaction();

		let table_shape = ShapeId::Table(TableId(1));
		let rb_shape = ShapeId::RingBuffer(RingBufferId(2));
		let series_shape = ShapeId::Series(SeriesId(3));

		let config_table = RowTtl {
			duration_nanos: 300_000_000_000,
			anchor: RowTtlAnchor::Created,
			cleanup_mode: RowTtlCleanupMode::Drop,
		};
		let config_rb = RowTtl {
			duration_nanos: 600_000_000_000,
			anchor: RowTtlAnchor::Updated,
			cleanup_mode: RowTtlCleanupMode::Delete,
		};
		let config_series = RowTtl {
			duration_nanos: 86_400_000_000_000,
			anchor: RowTtlAnchor::Created,
			cleanup_mode: RowTtlCleanupMode::Drop,
		};

		create_row_ttl(&mut txn, table_shape, &config_table).unwrap();
		create_row_ttl(&mut txn, rb_shape, &config_rb).unwrap();
		create_row_ttl(&mut txn, series_shape, &config_series).unwrap();

		let entries = CatalogStore::list_row_ttls(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(entries.len(), 3);
		assert!(entries.iter().any(|e| e.shape == table_shape && e.config == config_table));
		assert!(entries.iter().any(|e| e.shape == rb_shape && e.config == config_rb));
		assert!(entries.iter().any(|e| e.shape == series_shape && e.config == config_series));
	}
}
