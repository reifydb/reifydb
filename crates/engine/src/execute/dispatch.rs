// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::plan::physical::PhysicalPlan;
use reifydb_transaction::transaction::{
	Transaction, admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction,
};
use reifydb_type::params::Params;
use tracing::instrument;

use crate::{execute::Executor, stack::Stack};

impl Executor {
	#[instrument(name = "executor::dispatch_query", level = "debug", skip(self, rx, plan, params, stack))]
	pub(crate) fn dispatch_query<'a>(
		&self,
		rx: &'a mut QueryTransaction,
		plan: PhysicalPlan,
		params: Params,
		stack: &mut Stack,
	) -> crate::Result<Option<Columns>> {
		match plan {
			// Query
			PhysicalPlan::Aggregate(_)
			| PhysicalPlan::DictionaryScan(_)
			| PhysicalPlan::Filter(_)
			| PhysicalPlan::IndexScan(_)
			| PhysicalPlan::JoinInner(_)
			| PhysicalPlan::JoinLeft(_)
			| PhysicalPlan::JoinNatural(_)
			| PhysicalPlan::Take(_)
			| PhysicalPlan::Sort(_)
			| PhysicalPlan::Map(_)
			| PhysicalPlan::Extend(_)
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::Generator(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::FlowScan(_)
			| PhysicalPlan::TableVirtualScan(_)
			| PhysicalPlan::RingBufferScan(_)
			| PhysicalPlan::Variable(_)
			| PhysicalPlan::Environment(_)
			| PhysicalPlan::Conditional(_)
			| PhysicalPlan::Scalarize(_)
			| PhysicalPlan::RowPointLookup(_)
			| PhysicalPlan::RowListLookup(_)
			| PhysicalPlan::RowRangeScan(_) => {
				let mut std_txn = Transaction::from(rx);
				self.query(&mut std_txn, plan, params, stack)
			}
			// Mutations - should not be in query transactions
			PhysicalPlan::Delete(_)
			| PhysicalPlan::DeleteRingBuffer(_)
			| PhysicalPlan::InsertTable(_)
			| PhysicalPlan::InsertRingBuffer(_)
			| PhysicalPlan::InsertDictionary(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::UpdateRingBuffer(_) => {
				reifydb_type::err!(reifydb_core::error::diagnostic::internal::internal_with_context(
					"Mutation operations cannot be executed in a query transaction",
					file!(),
					line!(),
					column!(),
					module_path!(),
					module_path!()
				))
			}
			PhysicalPlan::Declare(_) | PhysicalPlan::Assign(_) => {
				let mut std_txn = Transaction::from(rx);
				self.query(&mut std_txn, plan, params, stack)?;
				Ok(None)
			}
			PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::AlterTable(_)
			| PhysicalPlan::AlterView(_)
			| PhysicalPlan::AlterFlow(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::CreateNamespace(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::CreateRingBuffer(_)
			| PhysicalPlan::CreateFlow(_)
			| PhysicalPlan::CreateDictionary(_)
			| PhysicalPlan::CreateSubscription(_)
			| PhysicalPlan::Distinct(_)
			| PhysicalPlan::Apply(_) => {
				// Apply operator requires flow engine for mod
				// execution
				unimplemented!(
					"Apply operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
				)
			}
			PhysicalPlan::Window(_) => {
				// Window operator requires flow engine for mod
				// execution
				unimplemented!(
					"Window operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
				)
			}
			PhysicalPlan::Merge(_) => {
				// Merge operator requires flow engine
				unimplemented!(
					"Merge operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
				)
			}
		}
	}

	#[instrument(name = "executor::dispatch_admin", level = "debug", skip(self, txn, plan, params, stack))]
	pub fn dispatch_admin<'a>(
		&self,
		txn: &'a mut AdminTransaction,
		plan: PhysicalPlan,
		params: Params,
		stack: &mut Stack,
	) -> crate::Result<Option<Columns>> {
		match plan {
			// DDL operations (admin only)
			PhysicalPlan::AlterSequence(plan) => Ok(Some(self.alter_table_sequence(txn, plan)?)),
			PhysicalPlan::CreateDeferredView(plan) => Ok(Some(self.create_deferred_view(txn, plan)?)),
			PhysicalPlan::CreateTransactionalView(plan) => {
				Ok(Some(self.create_transactional_view(txn, plan)?))
			}
			PhysicalPlan::CreateNamespace(plan) => Ok(Some(self.create_namespace(txn, plan)?)),
			PhysicalPlan::CreateTable(plan) => Ok(Some(self.create_table(txn, plan)?)),
			PhysicalPlan::CreateRingBuffer(plan) => Ok(Some(self.create_ringbuffer(txn, plan)?)),
			PhysicalPlan::CreateFlow(plan) => Ok(Some(self.create_flow(txn, plan)?)),
			PhysicalPlan::CreateDictionary(plan) => Ok(Some(self.create_dictionary(txn, plan)?)),
			PhysicalPlan::CreateSubscription(plan) => Ok(Some(self.create_subscription(txn, plan)?)),
			PhysicalPlan::AlterTable(plan) => Ok(Some(self.alter_table(txn, plan)?)),
			PhysicalPlan::AlterView(plan) => Ok(Some(self.execute_alter_view(txn, plan)?)),
			PhysicalPlan::AlterFlow(plan) => Ok(Some(self.execute_alter_flow(txn, plan)?)),

			// DML operations (via Transaction wrapper)
			PhysicalPlan::Delete(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.delete(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::DeleteRingBuffer(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.delete_ringbuffer(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::InsertTable(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.insert_table(&mut std_txn, plan, stack)?))
			}
			PhysicalPlan::InsertRingBuffer(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.insert_ringbuffer(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::InsertDictionary(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.insert_dictionary(&mut std_txn, plan, stack)?))
			}
			PhysicalPlan::Update(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.update_table(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::UpdateRingBuffer(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.update_ringbuffer(&mut std_txn, plan, params)?))
			}

			// Query operations
			PhysicalPlan::Aggregate(_)
			| PhysicalPlan::DictionaryScan(_)
			| PhysicalPlan::Filter(_)
			| PhysicalPlan::IndexScan(_)
			| PhysicalPlan::JoinInner(_)
			| PhysicalPlan::JoinLeft(_)
			| PhysicalPlan::JoinNatural(_)
			| PhysicalPlan::Take(_)
			| PhysicalPlan::Sort(_)
			| PhysicalPlan::Map(_)
			| PhysicalPlan::Extend(_)
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::Generator(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::FlowScan(_)
			| PhysicalPlan::TableVirtualScan(_)
			| PhysicalPlan::RingBufferScan(_)
			| PhysicalPlan::Distinct(_)
			| PhysicalPlan::Variable(_)
			| PhysicalPlan::Environment(_)
			| PhysicalPlan::Apply(_)
			| PhysicalPlan::Conditional(_)
			| PhysicalPlan::Scalarize(_)
			| PhysicalPlan::RowPointLookup(_)
			| PhysicalPlan::RowListLookup(_)
			| PhysicalPlan::RowRangeScan(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)
			}
			PhysicalPlan::Declare(_) | PhysicalPlan::Assign(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)?;
				Ok(None)
			}
			PhysicalPlan::Window(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)
			}
			PhysicalPlan::Merge(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)
			}
		}
	}

	#[instrument(name = "executor::dispatch_command", level = "debug", skip(self, txn, plan, params, stack))]
	pub fn dispatch_command<'a>(
		&self,
		txn: &'a mut CommandTransaction,
		plan: PhysicalPlan,
		params: Params,
		stack: &mut Stack,
	) -> crate::Result<Option<Columns>> {
		match plan {
			// DML operations (via Transaction wrapper)
			PhysicalPlan::Delete(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.delete(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::DeleteRingBuffer(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.delete_ringbuffer(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::InsertTable(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.insert_table(&mut std_txn, plan, stack)?))
			}
			PhysicalPlan::InsertRingBuffer(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.insert_ringbuffer(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::InsertDictionary(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.insert_dictionary(&mut std_txn, plan, stack)?))
			}
			PhysicalPlan::Update(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.update_table(&mut std_txn, plan, params)?))
			}
			PhysicalPlan::UpdateRingBuffer(plan) => {
				let mut std_txn = Transaction::from(txn);
				Ok(Some(self.update_ringbuffer(&mut std_txn, plan, params)?))
			}

			// Query operations
			PhysicalPlan::Aggregate(_)
			| PhysicalPlan::DictionaryScan(_)
			| PhysicalPlan::Filter(_)
			| PhysicalPlan::IndexScan(_)
			| PhysicalPlan::JoinInner(_)
			| PhysicalPlan::JoinLeft(_)
			| PhysicalPlan::JoinNatural(_)
			| PhysicalPlan::Take(_)
			| PhysicalPlan::Sort(_)
			| PhysicalPlan::Map(_)
			| PhysicalPlan::Extend(_)
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::Generator(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::FlowScan(_)
			| PhysicalPlan::TableVirtualScan(_)
			| PhysicalPlan::RingBufferScan(_)
			| PhysicalPlan::Distinct(_)
			| PhysicalPlan::Variable(_)
			| PhysicalPlan::Environment(_)
			| PhysicalPlan::Apply(_)
			| PhysicalPlan::Conditional(_)
			| PhysicalPlan::Scalarize(_)
			| PhysicalPlan::RowPointLookup(_)
			| PhysicalPlan::RowListLookup(_)
			| PhysicalPlan::RowRangeScan(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)
			}
			PhysicalPlan::Declare(_) | PhysicalPlan::Assign(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)?;
				Ok(None)
			}
			PhysicalPlan::Window(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)
			}
			PhysicalPlan::Merge(_) => {
				let mut std_txn = Transaction::from(txn);
				self.query(&mut std_txn, plan, params, stack)
			}

			// DDL operations - not allowed in command transactions
			PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::CreateNamespace(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::CreateRingBuffer(_)
			| PhysicalPlan::CreateFlow(_)
			| PhysicalPlan::CreateDictionary(_)
			| PhysicalPlan::CreateSubscription(_)
			| PhysicalPlan::AlterTable(_)
			| PhysicalPlan::AlterView(_)
			| PhysicalPlan::AlterFlow(_) => {
				reifydb_type::err!(reifydb_core::error::diagnostic::internal::internal_with_context(
					"DDL operations require an admin transaction",
					file!(),
					line!(),
					column!(),
					module_path!(),
					module_path!()
				))
			}
		}
	}
}
