// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::series::SeriesToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateSeriesNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_series(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSeriesNode,
) -> crate::Result<Columns> {
	services.catalog.create_series(
		txn,
		SeriesToCreate {
			name: plan.series.clone(),
			namespace: plan.namespace.def().id,
			columns: plan.columns,
			tag: plan.tag,
			precision: plan.precision,
		},
	)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("series", Value::Utf8(plan.series.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
