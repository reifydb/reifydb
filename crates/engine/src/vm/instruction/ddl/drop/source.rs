// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropSourceNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_source(services: &Services, txn: &mut AdminTransaction, plan: DropSourceNode) -> Result<Columns> {
	let source = services.catalog.find_source_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.id(),
		plan.name.text(),
	)?;

	let Some(source) = source else {
		if plan.if_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("source", Value::Utf8(plan.name.text().to_string())),
				("dropped", Value::Boolean(false)),
			]));
		}
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Source,
			namespace: plan.namespace.name().to_string(),
			name: plan.name.text().to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	};

	services.catalog.drop_source(txn, source)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("source", Value::Utf8(plan.name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
