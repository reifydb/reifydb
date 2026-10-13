// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::{bytes::EncodedBytes, pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::{
		id::SeriesId,
		series::{SeriesPartitionMetadata, decode_series_partition_metadata, encode_series_partition_metadata},
	},
	key::series::SeriesPartitionMetadataKey,
};
use reifydb_transaction::{multi::transaction::write::Rebase, transaction::Transaction};
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

	pub(crate) fn update_series_metadata_rebased_txn(
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		partition: Partition,
		metadata: SeriesPartitionMetadata,
		change: impl Fn(&mut SeriesPartitionMetadata) -> Result<()> + Send + Sync + 'static,
	) -> Result<()> {
		let row = encode_series_partition_metadata(&metadata);
		let rebase: Rebase = Arc::new(move |bytes: &EncodedBytes| {
			let mut latest = decode_series_partition_metadata(EncodedPodRow::view(bytes))?;
			change(&mut latest)?;
			Ok(encode_series_partition_metadata(&latest).into_bytes())
		});
		txn.set_rebased(&SeriesPartitionMetadataKey::new(series_id, partition), row.into_bytes(), rebase)?;
		Ok(())
	}
}
