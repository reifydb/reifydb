// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::SeriesId,
	key::{
		any::TaggedKey,
		namespace::NamespaceSeriesKey,
		series::{SeriesKey, SeriesPartitionMetadataKey},
	},
};
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction},
};

use crate::{CatalogStore, Result, store::object::drop::drop_object_metadata};

impl CatalogStore {
	pub(crate) fn drop_series(txn: &mut AdminTransaction, series: SeriesId) -> Result<()> {
		let pk_id = if let Some(series_def) = Self::find_series(&mut Transaction::Admin(&mut *txn), series)? {
			txn.remove(&NamespaceSeriesKey::new(series_def.namespace, series))?;
			series_def.primary_key.as_ref().map(|pk| pk.id)
		} else {
			None
		};

		drop_object_metadata(txn, series.into(), pk_id)?;

		Self::drop_series_partition_metadata(txn, series)?;

		txn.remove(&SeriesKey::new(series))?;

		Ok(())
	}

	fn drop_series_partition_metadata(txn: &mut AdminTransaction, series: SeriesId) -> Result<()> {
		let mut keys: Vec<SeriesPartitionMetadataKey> = Vec::new();
		{
			let stream = txn.range(SeriesPartitionMetadataKey::full_scan(series), RangeScope::All, 1024)?;
			for entry in stream {
				let entry = entry?;
				if let TaggedKey::SeriesPartitionMetadata(k) = entry.key {
					keys.push(k);
				}
			}
		}
		for key in keys {
			txn.remove(&key)?;
		}
		Ok(())
	}
}
