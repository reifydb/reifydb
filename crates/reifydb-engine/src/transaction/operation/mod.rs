// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod schema_def;
mod table;
mod table_def;
mod view_def;

use crate::StandardCommandTransaction;
use reifydb_core::interface::{CommandTransaction, SchemaId, Transaction};
use reifydb_core::{error, internal_error};
pub(crate) use schema_def::*;
pub(crate) use table::*;

/// Helper to get schema name from SchemaId
pub(crate) fn get_schema_name<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	schema_id: SchemaId,
) -> crate::Result<String> {
	txn.get_changes_mut()
		.schema_def
		.get(&schema_id)
		.and_then(|change| change.post.as_ref().or(change.pre.as_ref()))
		.map(|schema| schema.name.clone())
		.ok_or_else(|| {
			error!(internal_error!(
				"Schema {} not found in transaction changes - this should never happen",
				schema_id
			))
		})
}
