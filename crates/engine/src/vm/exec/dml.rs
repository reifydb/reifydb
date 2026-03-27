// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::params::Params;

use crate::{
	Result,
	vm::{
		services::Services,
		stack::{SymbolTable, Variable},
		vm::Vm,
	},
};

fn reject_query_txn(tx: &Transaction<'_>) -> Result<()> {
	if matches!(tx, Transaction::Query(_)) {
		return Err(internal_error!("Mutation operations cannot be executed in a query transaction"));
	}
	Ok(())
}

impl Vm {
	/// Execute a DML operation that takes params and a read-only reference to the symbol table.
	/// Used by Delete, Update, InsertRingBuffer, InsertSeries, etc.
	pub(crate) fn exec_dml_with_params<F>(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		params: &Params,
		handler: F,
	) -> Result<()>
	where
		F: FnOnce(&Arc<Services>, &mut Transaction<'_>, Params, &SymbolTable) -> Result<Columns>,
	{
		reject_query_txn(tx)?;
		let mut txn = tx.reborrow();
		let columns = handler(services, &mut txn, params.clone(), &self.symbols)?;
		self.stack.push(Variable::Columns(columns));
		Ok(())
	}

	/// Execute a DML operation that takes a mutable reference to the symbol table (no params).
	/// Used by InsertTable, InsertDictionary.
	pub(crate) fn exec_dml_with_mut_symbols<F>(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		handler: F,
	) -> Result<()>
	where
		F: FnOnce(&Arc<Services>, &mut Transaction<'_>, &mut SymbolTable) -> Result<Columns>,
	{
		reject_query_txn(tx)?;
		let mut txn = tx.reborrow();
		let columns = handler(services, &mut txn, &mut self.symbols)?;
		self.stack.push(Variable::Columns(columns));
		Ok(())
	}
}
