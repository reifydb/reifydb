// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{catalog::series::SeriesToCreate, store::row_settings::create::create_row_settings};
use reifydb_core::{interface::catalog::shape::ShapeId, row::RowSettings, value::column::columns::Columns};
use reifydb_rql::nodes::CreateSeriesNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::Value;

use super::require_buffer_for_non_persistent;
use crate::{Result, vm::services::Services};

pub(crate) fn create_series(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSeriesNode,
) -> Result<Columns> {
	require_buffer_for_non_persistent(txn, plan.persistent, plan.series.clone(), plan.series.text())?;

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
		create_row_settings(
			txn,
			ShapeId::Series(result.id),
			&RowSettings {
				ttl: Some(ttl),
				persistent: plan.persistent,
			},
		)?;
	}

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("series", Value::Utf8(plan.series.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
