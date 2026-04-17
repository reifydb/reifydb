// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropBindingNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_binding(services: &Services, txn: &mut AdminTransaction, plan: DropBindingNode) -> Result<Columns> {
	let binding = services.catalog.find_binding_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.id(),
		plan.name.text(),
	)?;

	let Some(binding) = binding else {
		if plan.if_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("binding", Value::Utf8(plan.name.text().to_string())),
				("dropped", Value::Boolean(false)),
			]));
		}
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Procedure,
			namespace: plan.namespace.name().to_string(),
			name: plan.name.text().to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	};

	services.catalog.drop_binding(txn, binding.id)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("binding", Value::Utf8(plan.name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
