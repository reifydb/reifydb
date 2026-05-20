// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::test::TestToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateTestNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_test(services: &Services, txn: &mut AdminTransaction, plan: CreateTestNode) -> Result<Columns> {
	let test = services.catalog.create_test(
		txn,
		TestToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			cases: plan.cases,
			body: plan.body_source,
		},
	)?;

	Ok(Columns::single_row([("test", Value::Utf8(test.name)), ("created", Value::Boolean(true))]))
}
