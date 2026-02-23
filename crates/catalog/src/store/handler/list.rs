// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::handler::HandlerDef, key::handler::HandlerKey};
use reifydb_transaction::transaction::Transaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn list_all_handlers(rx: &mut Transaction<'_>) -> crate::Result<Vec<HandlerDef>> {
		let mut results = Vec::new();

		let mut stream = rx.range(HandlerKey::full_scan(), 1024)?;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			results.push(super::handler_def_from_row(&multi.values));
		}

		Ok(results)
	}
}
