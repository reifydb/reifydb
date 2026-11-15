// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ProcedureId, procedure::Procedure},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::ValueError;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn get_procedure(rx: &mut Transaction<'_>, procedure: ProcedureId) -> Result<Procedure, ValueError> {
		CatalogStore::find_procedure(rx, procedure)?.ok_or_else(|| {
			ValueError(Box::new(internal!(
				"Procedure with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				procedure
			)))
		})
	}
}
