// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{flow::operator::OperatorDef, value::batch::single_row};
use reifydb_rql::nodes::DropTableNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use super::dependent::find_flow_dependents;
use crate::{Result, vm::services::Services};

pub(crate) fn drop_table(services: &Services, txn: &mut AdminTransaction, plan: DropTableNode) -> Result<RecordBatch> {
	let Some(table_id) = plan.table_id else {
		return single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("table", Value::Utf8(plan.table_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]);
	};

	let def = services.catalog.get_table(&mut Transaction::Admin(txn), table_id)?;

	let dags = services.catalog.list_flow_dags_asc(&mut Transaction::Admin(txn))?;
	let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;
	let dependents = find_flow_dependents(
		&services.catalog,
		txn,
		&dags,
		&flows,
		|node_type| matches!(node_type, OperatorDef::SourceTable { table, .. } if *table == table_id),
	)?;
	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return Err(CatalogError::InUse {
			kind: CatalogObjectKind::Table,
			namespace: plan.namespace_name.text().to_string(),
			name: Some(plan.table_name.text().to_string()),
			dependents: dependents_str,
			fragment: plan.table_name.clone(),
		}
		.into());
	}

	services.catalog.drop_table(txn, def)?;

	single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("table", Value::Utf8(plan.table_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	])
}
