// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SinkId, sink::Sink},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_sink(rx: &mut Transaction<'_>, sink: SinkId) -> Result<Sink> {
		CatalogStore::find_sink(rx, sink)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Sink with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sink
			)))
		})
	}
}
