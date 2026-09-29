// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::CreateRoleNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_role(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateRoleNode,
) -> Result<RecordBatch> {
	let name = plan.name.text();

	services.catalog.create_role(txn, name)?;

	single_row([("role", Value::Utf8(name.to_string())), ("created", Value::Boolean(true))])
}
