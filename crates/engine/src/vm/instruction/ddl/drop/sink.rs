// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropSinkNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_sink(services: &Services, txn: &mut AdminTransaction, plan: DropSinkNode) -> Result<Columns> {
	let sink = services.catalog.find_sink_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.id(),
		plan.name.text(),
	)?;

	let Some(sink) = sink else {
		if plan.if_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("sink", Value::Utf8(plan.name.text().to_string())),
				("dropped", Value::Boolean(false)),
			]));
		}
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Sink,
			namespace: plan.namespace.name().to_string(),
			name: plan.name.text().to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	};

	services.catalog.drop_sink(txn, sink)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("sink", Value::Utf8(plan.name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
