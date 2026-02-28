// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateRoleNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_role(services: &Services, txn: &mut AdminTransaction, plan: CreateRoleNode) -> Result<Columns> {
	let name = plan.name.text();

	services.catalog.create_role(txn, name)?;

	Ok(Columns::single_row([("role", Value::Utf8(name.to_string())), ("created", Value::Boolean(true))]))
}
