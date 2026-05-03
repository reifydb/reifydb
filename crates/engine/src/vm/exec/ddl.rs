// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	Result,
	vm::{services::Services, stack::Variable, vm::Vm},
};

pub(crate) fn require_admin_txn<'a>(tx: &'a mut Transaction<'_>) -> Result<&'a mut AdminTransaction> {
	match tx {
		Transaction::Admin(txn) => Ok(txn),
		Transaction::Test(t) => Ok(&mut *t.inner),
		_ => Err(internal_error!("DDL operations require an admin transaction")),
	}
}

impl<'a> Vm<'a> {
	pub(crate) fn exec_ddl<F>(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		handler: F,
	) -> Result<()>
	where
		F: FnOnce(&Services, &mut AdminTransaction) -> Result<Columns>,
	{
		let txn = require_admin_txn(tx)?;
		let columns = handler(services, txn)?;
		self.stack.push(Variable::columns(columns));
		Ok(())
	}

	pub(crate) fn exec_ddl_sub<F>(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		handler: F,
	) -> Result<()>
	where
		F: FnOnce(&Services, &mut Transaction<'_>) -> Result<Columns>,
	{
		let columns = handler(services, tx)?;
		self.stack.push(Variable::columns(columns));
		Ok(())
	}
}
