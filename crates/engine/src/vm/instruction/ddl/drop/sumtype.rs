// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropSumTypeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::{Value, constraint::Constraint};

use super::dependent::find_column_dependents;
use crate::{Result, vm::services::Services};

pub(crate) fn drop_sumtype(services: &Services, txn: &mut AdminTransaction, plan: DropSumTypeNode) -> Result<Columns> {
	let Some(sumtype_id) = plan.sumtype_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("enum", Value::Utf8(plan.sumtype_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_sumtype(&mut Transaction::Admin(txn), sumtype_id)?;

	// Check for dependent columns across all entity types
	let columns = services.catalog.list_columns_all(&mut Transaction::Admin(txn))?;
	let dependents = find_column_dependents(&services.catalog, txn, &columns, |info| {
		if let Some(Constraint::SumType(id)) = info.column.constraint.constraint() {
			if *id == sumtype_id {
				return Some(String::new());
			}
		}
		None
	})?;
	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return Err(CatalogError::InUse {
			kind: CatalogObjectKind::Enum,
			namespace: plan.namespace_name.text().to_string(),
			name: Some(plan.sumtype_name.text().to_string()),
			dependents: dependents_str,
			fragment: plan.sumtype_name.clone(),
		}
		.into());
	}

	services.catalog.drop_sumtype(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("enum", Value::Utf8(plan.sumtype_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
