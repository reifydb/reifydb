// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc};

use reifydb_catalog::{catalog::Catalog, vtable::system::flow_operator_store::FlowOperatorStore};
use reifydb_core::util::ioc::IocContainer;
use reifydb_function::registry::Functions;
use reifydb_metric::metric::MetricReader;
use reifydb_rql::compiler::CompilationResult;
use reifydb_runtime::clock::Clock;
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::{admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction};
use reifydb_type::{params::Params, value::frame::frame::Frame};
use tracing::instrument;

use crate::vm::{
	Admin, Command, Query,
	interpret::TransactionAccess,
	services::Services,
	stack::{SymbolTable, Variable},
	vm::Vm,
};

/// Executor is the orchestration layer for RQL statement execution.
pub struct Executor(Arc<Services>);

impl Clone for Executor {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Deref for Executor {
	type Target = Services;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Executor {
	pub fn new(
		catalog: Catalog,
		clock: Clock,
		functions: Functions,
		flow_operator_store: FlowOperatorStore,
		stats_reader: MetricReader<SingleStore>,
		ioc: IocContainer,
	) -> Self {
		Self(Arc::new(Services::new(catalog, clock, functions, flow_operator_store, stats_reader, ioc)))
	}

	/// Get a reference to the underlying Services
	pub fn services(&self) -> &Arc<Services> {
		&self.0
	}

	#[allow(dead_code)]
	pub fn testing() -> Self {
		Self(Services::testing())
	}
}

/// Populate a stack with parameters so they can be accessed as variables.
fn populate_stack(stack: &mut SymbolTable, params: &Params) -> crate::Result<()> {
	match params {
		Params::Positional(values) => {
			for (index, value) in values.iter().enumerate() {
				let param_name = (index + 1).to_string();
				stack.set(param_name, Variable::scalar(value.clone()), false)?;
			}
		}
		Params::Named(map) => {
			for (name, value) in map {
				stack.set(name.clone(), Variable::scalar(value.clone()), false)?;
			}
		}
		Params::None => {}
	}
	Ok(())
}

impl Executor {
	#[instrument(name = "executor::admin", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn admin(&self, txn: &mut AdminTransaction, cmd: Admin<'_>) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbol_table = SymbolTable::new();
		populate_stack(&mut symbol_table, &cmd.params)?;

		match self.compiler.compile(txn, cmd.rql)? {
			CompilationResult::Ready(compiled) => {
				for compiled in compiled.iter() {
					result.clear();
					let mut tx = TransactionAccess::Admin(txn);
					let mut vm = Vm::new(symbol_table);
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					symbol_table = vm.symbol_table;

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
			}
			CompilationResult::Incremental(mut state) => {
				while let Some(compiled) = self.compiler.compile_next(txn, &mut state)? {
					result.clear();
					let mut tx = TransactionAccess::Admin(txn);
					let mut vm = Vm::new(symbol_table);
					vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
					symbol_table = vm.symbol_table;

					if compiled.is_output {
						output_results.append(&mut result);
					}
				}
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}

	#[instrument(name = "executor::command", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn command(&self, txn: &mut CommandTransaction, cmd: Command<'_>) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbol_table = SymbolTable::new();
		populate_stack(&mut symbol_table, &cmd.params)?;

		let compiled = match self.compiler.compile(txn, cmd.rql)? {
			CompilationResult::Ready(compiled) => compiled,
			CompilationResult::Incremental(_) => {
				unreachable!("DDL statements require admin transactions, not command transactions")
			}
		};

		for compiled in compiled.iter() {
			result.clear();
			let mut tx = TransactionAccess::Command(txn);
			let mut vm = Vm::new(symbol_table);
			vm.run(&self.0, &mut tx, &compiled.instructions, &cmd.params, &mut result)?;
			symbol_table = vm.symbol_table;

			if compiled.is_output {
				output_results.append(&mut result);
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}

	#[instrument(name = "executor::query", level = "debug", skip(self, txn, qry), fields(rql = %qry.rql))]
	pub fn query(&self, txn: &mut QueryTransaction, qry: Query<'_>) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let mut output_results: Vec<Frame> = Vec::new();
		let mut symbol_table = SymbolTable::new();
		populate_stack(&mut symbol_table, &qry.params)?;

		let compiled = match self.compiler.compile(txn, qry.rql)? {
			CompilationResult::Ready(compiled) => compiled,
			CompilationResult::Incremental(_) => {
				unreachable!("DDL statements require admin transactions, not query transactions")
			}
		};

		for compiled in compiled.iter() {
			result.clear();
			let mut tx = TransactionAccess::Query(txn);
			let mut vm = Vm::new(symbol_table);
			vm.run(&self.0, &mut tx, &compiled.instructions, &qry.params, &mut result)?;
			symbol_table = vm.symbol_table;

			if compiled.is_output {
				output_results.append(&mut result);
			}
		}

		let mut final_result = output_results;
		final_result.append(&mut result);
		Ok(final_result)
	}
}
