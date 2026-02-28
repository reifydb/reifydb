// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropSeriesNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_series(services: &Services, txn: &mut AdminTransaction, plan: DropSeriesNode) -> Result<Columns> {
	let Some(series_id) = plan.series_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("series", Value::Utf8(plan.series_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_series(&mut Transaction::Admin(txn), series_id)?;

	services.catalog.drop_series(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("series", Value::Utf8(plan.series_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
