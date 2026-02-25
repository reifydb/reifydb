// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::SeriesId,
	key::{
		namespace_series::NamespaceSeriesKey,
		series::{SeriesKey, SeriesMetadataKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, store::primitive::drop::drop_primitive_metadata};

impl CatalogStore {
	pub(crate) fn drop_series(txn: &mut AdminTransaction, series: SeriesId) -> crate::Result<()> {
		// First, find the series to get its namespace and primary key
		let pk_id = if let Some(series_def) = Self::find_series(&mut Transaction::Admin(&mut *txn), series)? {
			// Remove the namespace-series link (secondary index)
			txn.remove(&NamespaceSeriesKey::encoded(series_def.namespace, series))?;
			series_def.primary_key.as_ref().map(|pk| pk.id)
		} else {
			None
		};

		// Clean up all associated metadata (columns, policies, sequences, pk)
		drop_primitive_metadata(txn, series.into(), pk_id)?;

		// Remove the series metadata
		txn.remove(&SeriesMetadataKey::encoded(series))?;

		// Remove the series definition
		txn.remove(&SeriesKey::encoded(series))?;

		Ok(())
	}
}
