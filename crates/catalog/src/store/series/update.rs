// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow, interface::catalog::series::SeriesMetadata, key::series::SeriesMetadataKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::series::shape::series_metadata};

fn encode_series_metadata(metadata: &SeriesMetadata) -> EncodedRow {
	let mut row = series_metadata::SHAPE.allocate();
	series_metadata::SHAPE.set_u64(&mut row, series_metadata::ID, metadata.id.0);
	series_metadata::SHAPE.set_u64(&mut row, series_metadata::ROW_COUNT, metadata.row_count);
	series_metadata::SHAPE.set_u64(&mut row, series_metadata::OLDEST_KEY, metadata.oldest_key);
	series_metadata::SHAPE.set_u64(&mut row, series_metadata::NEWEST_KEY, metadata.newest_key);
	series_metadata::SHAPE.set_u64(&mut row, series_metadata::SEQUENCE_COUNTER, metadata.sequence_counter);
	row
}

impl CatalogStore {
	pub(crate) fn update_series_metadata_txn(txn: &mut Transaction<'_>, metadata: SeriesMetadata) -> Result<()> {
		let row = encode_series_metadata(&metadata);
		txn.set(&SeriesMetadataKey::encoded(metadata.id), row)?;
		Ok(())
	}
}
