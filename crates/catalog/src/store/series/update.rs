// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{interface::catalog::series::SeriesMetadata, key::series::SeriesMetadataKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::series::shape::series_metadata};

fn encode_series_metadata(metadata: &SeriesMetadata) -> EncodedBytes {
	let mut row = series_metadata::allocate();
	series_metadata::set_id(&mut row, metadata.id.0);
	series_metadata::set_row_count(&mut row, metadata.row_count);
	series_metadata::set_oldest_key(&mut row, metadata.oldest_key);
	series_metadata::set_newest_key(&mut row, metadata.newest_key);
	series_metadata::set_sequence_counter(&mut row, metadata.sequence_counter);
	row.freeze()
}

impl CatalogStore {
	pub(crate) fn update_series_metadata_txn(txn: &mut Transaction<'_>, metadata: SeriesMetadata) -> Result<()> {
		let row = encode_series_metadata(&metadata);
		txn.set(&SeriesMetadataKey::encoded(metadata.id), row)?;
		Ok(())
	}
}
