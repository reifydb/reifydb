// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::DropSeriesNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_series(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropSeriesNode,
) -> Result<RecordBatch> {
	let Some(series_id) = plan.series_id else {
		return single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("series", Value::Utf8(plan.series_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]);
	};

	let def = services.catalog.get_series(&mut Transaction::Admin(txn), series_id)?;

	services.catalog.drop_series(txn, def)?;

	single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("series", Value::Utf8(plan.series_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	])
}
