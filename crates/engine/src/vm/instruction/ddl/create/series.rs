// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{catalog::series::SeriesToCreate, store::row_settings::create::create_row_settings};
use reifydb_core::{interface::catalog::storage::StorageId, row::RowSettings, value::column::columns::Columns};
use reifydb_rql::nodes::CreateSeriesNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::Value;

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
			partition_by: plan.partition_by.clone(),
			time: plan.time.clone(),
		},
	)?;

	if let Some(ttl) = plan.ttl {
		create_row_settings(
			txn,
			StorageId::Series(result.id),
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
