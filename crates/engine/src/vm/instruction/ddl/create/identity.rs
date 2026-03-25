// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateIdentityNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_identity(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateIdentityNode,
) -> Result<Columns> {
	let name = plan.name.text();

	services.catalog.create_identity(txn, name)?;

	Ok(Columns::single_row([("identity", Value::Utf8(name.to_string())), ("created", Value::Boolean(true))]))
}
