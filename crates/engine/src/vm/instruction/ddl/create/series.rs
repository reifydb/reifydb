// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{catalog::series::SeriesToCreate, store::ttl::create::create_row_ttl};
use reifydb_core::{interface::catalog::shape::ShapeId, value::column::columns::Columns};
use reifydb_rql::nodes::CreateSeriesNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_series(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSeriesNode,
) -> Result<Columns> {
	let result = services.catalog.create_series(
		txn,
		SeriesToCreate {
			name: plan.series.clone(),
			namespace: plan.namespace.def().id(),
			columns: plan.columns,
			tag: plan.tag,
			key: plan.key,
			underlying: false,
		},
	)?;

	if let Some(ttl) = plan.ttl {
		create_row_ttl(txn, ShapeId::Series(result.id), &ttl)?;
	}

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("series", Value::Utf8(plan.series.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
