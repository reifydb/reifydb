// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::SeriesId,
		series::{SeriesPartitionMetadata, encode_series_partition_metadata},
	},
	key::series::SeriesPartitionMetadataKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::partition::Partition;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn update_series_metadata_txn(
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		partition: Partition,
		metadata: SeriesPartitionMetadata,
	) -> Result<()> {
		let row = encode_series_partition_metadata(&metadata);
		txn.set(&SeriesPartitionMetadataKey::new(series_id, partition), row.into_bytes())?;
		Ok(())
	}
}
