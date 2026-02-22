// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{catalog::Catalog, store::column::list::ColumnInfo};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

pub(crate) fn find_column_dependents(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	columns: &[ColumnInfo],
	check: impl Fn(&ColumnInfo) -> Option<String>,
) -> crate::Result<Vec<String>> {
	let mut dependents = Vec::new();
	for info in columns {
		if let Some(suffix) = check(info) {
			let ns = catalog.find_namespace(&mut Transaction::Admin(txn), info.namespace)?;
			let ns_name = ns.map(|n| n.name).unwrap_or_else(|| "?".to_string());
			let mut desc = format!(
				"column `{}` in {} `{}.{}`",
				info.column.name, info.entity_kind, ns_name, info.entity_name
			);
			if !suffix.is_empty() {
				desc.push_str(&suffix);
			}
			dependents.push(desc);
		}
	}
	Ok(dependents)
}
