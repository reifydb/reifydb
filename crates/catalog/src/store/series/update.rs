// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::encoded::EncodedValues, interface::catalog::series::SeriesMetadata, key::series::SeriesMetadataKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::series::schema::series_metadata};

fn encode_series_metadata(metadata: &SeriesMetadata) -> EncodedValues {
	let mut row = series_metadata::SCHEMA.allocate();
	series_metadata::SCHEMA.set_u64(&mut row, series_metadata::ID, metadata.id.0);
	series_metadata::SCHEMA.set_u64(&mut row, series_metadata::ROW_COUNT, metadata.row_count);
	series_metadata::SCHEMA.set_i64(&mut row, series_metadata::OLDEST_TIMESTAMP, metadata.oldest_timestamp);
	series_metadata::SCHEMA.set_i64(&mut row, series_metadata::NEWEST_TIMESTAMP, metadata.newest_timestamp);
	series_metadata::SCHEMA.set_u64(&mut row, series_metadata::SEQUENCE_COUNTER, metadata.sequence_counter);
	row
}

impl CatalogStore {
	pub(crate) fn update_series_metadata_txn(txn: &mut Transaction<'_>, metadata: SeriesMetadata) -> Result<()> {
		let row = encode_series_metadata(&metadata);
		txn.set(&SeriesMetadataKey::encoded(metadata.id), row)?;
		Ok(())
	}
}
