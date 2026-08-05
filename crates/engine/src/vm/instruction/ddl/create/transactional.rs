// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateTransactionalViewNode;
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{Result, vm::services::Services};

pub(crate) fn create_transactional_view(
	_services: &Services,
	_txn: &mut AdminTransaction,
	_plan: CreateTransactionalViewNode,
) -> Result<Columns> {
	unimplemented!("transactional view execution; see plan-operator.md follow-up")
}
