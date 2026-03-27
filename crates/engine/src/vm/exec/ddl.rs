// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	Result,
	vm::{services::Services, stack::Variable, vm::Vm},
};

/// Extract an AdminTransaction from the current transaction context.
/// DDL operations require either an Admin or Test transaction.
pub(crate) fn require_admin_txn<'a>(tx: &'a mut Transaction<'_>) -> Result<&'a mut AdminTransaction> {
	match tx {
		Transaction::Admin(txn) => Ok(txn),
		Transaction::Test(t) => Ok(&mut *t.inner),
		_ => Err(internal_error!("DDL operations require an admin transaction")),
	}
}

/// Variant that also accepts Subscription transactions (for CreateSubscription/DropSubscription).
pub(crate) fn require_admin_or_subscription_txn<'a>(tx: &'a mut Transaction<'_>) -> Result<&'a mut AdminTransaction> {
	match tx {
		Transaction::Admin(txn) => Ok(txn),
		Transaction::Subscription(txn) => Ok(txn.as_admin_mut()),
		Transaction::Test(t) => Ok(&mut *t.inner),
		_ => Err(internal_error!("DDL operations require an admin transaction")),
	}
}

impl Vm {
	/// Execute a DDL operation that requires an AdminTransaction.
	/// Extracts the transaction, calls the handler, and pushes the result onto the stack.
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
		self.stack.push(Variable::Columns(columns));
		Ok(())
	}

	/// Execute a DDL operation that also accepts Subscription transactions.
	pub(crate) fn exec_ddl_sub<F>(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		handler: F,
	) -> Result<()>
	where
		F: FnOnce(&Services, &mut AdminTransaction) -> Result<Columns>,
	{
		let txn = require_admin_or_subscription_txn(tx)?;
		let columns = handler(services, txn)?;
		self.stack.push(Variable::Columns(columns));
		Ok(())
	}
}
