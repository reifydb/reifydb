// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ProcedureId, procedure::Procedure},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_procedure(rx: &mut Transaction<'_>, procedure: ProcedureId) -> Result<Procedure> {
		CatalogStore::find_procedure(rx, procedure)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Procedure with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				procedure
			)))
		})
	}
}
