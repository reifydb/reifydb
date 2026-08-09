// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::SeriesId,
		series::{SeriesMetadata, encode_series_metadata},
	},
	key::series::SeriesMetadataKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn update_series_metadata_txn(
		txn: &mut Transaction<'_>,
		series_id: SeriesId,
		metadata: SeriesMetadata,
	) -> Result<()> {
		let row = encode_series_metadata(&metadata);
		txn.set(&SeriesMetadataKey::encoded(series_id), row.into_bytes())?;
		Ok(())
	}
}
