// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SinkId, sink::Sink},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_sink(rx: &mut Transaction<'_>, sink: SinkId) -> Result<Sink> {
		CatalogStore::find_sink(rx, sink)?.ok_or_else(|| {
			Error(internal!(
				"Sink with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sink
			))
		})
	}
}
