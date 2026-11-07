// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::{
		id::SeriesId,
		series::{SeriesPartitionMetadata, decode_series_partition_metadata, encode_series_partition_metadata},
	},
	key::{any::TaggedKey, series::SeriesPartitionMetadataKey},
};
use reifydb_transaction::{multi::transaction::write::Rebase, transaction::Transaction};
use reifydb_value::value::partition::Partition;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn update_series_metadata_rebased_txn<F>(
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		updates: Vec<(Partition, SeriesPartitionMetadata, F)>,
	) -> Result<()>
	where
		F: Fn(&mut SeriesPartitionMetadata) -> Result<()> + Send + Sync + 'static,
	{
		let entries: Vec<(TaggedKey, EncodedBytes, Rebase)> = updates
			.into_iter()
			.map(|(partition, metadata, change)| {
				let rebase: Rebase = Arc::new(move |bytes: &EncodedBytes| {
					let mut latest = decode_series_partition_metadata(EncodedPodRow::view(bytes))?;
					change(&mut latest)?;
					Ok(encode_series_partition_metadata(&latest).into_bytes())
				});
				(
					SeriesPartitionMetadataKey::new(series_id, partition).into(),
					encode_series_partition_metadata(&metadata).into_bytes(),
					rebase,
				)
			})
			.collect();
		txn.set_rebased(entries)?;
		Ok(())
	}
}
