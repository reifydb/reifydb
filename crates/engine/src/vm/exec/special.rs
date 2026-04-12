// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	internal_error,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::{
	compiler::CompilationResult,
	nodes::{AssertBlockNode, DispatchNode, MigrateNode, RollbackMigrationNode},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error::Diagnostic, fragment::Fragment, params::Params, value::r#type::Type};

use crate::{
	Result,
	error::EngineError,
	vm::{
		instruction::{
			ddl::migrate::{migrate::execute_migrate, rollback::execute_rollback_migration},
			dml::dispatch::dispatch,
		},
		services::Services,
		stack::Variable,
		vm::Vm,
	},
};

impl Vm {
	pub(crate) fn exec_dispatch(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		node: &DispatchNode,
		params: &Params,
	) -> Result<()> {
		if matches!(tx, Transaction::Query(_)) {
			return Err(internal_error!("DISPATCH requires a command or admin transaction"));
		}
		let depth = self.dispatch_depth;
		self.dispatch_depth += 1;
		let columns = dispatch(self, services, tx, node.clone(), params, depth)?;
		self.dispatch_depth -= 1;
		self.stack.push(Variable::columns(columns));
		Ok(())
	}

	pub(crate) fn exec_migrate(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		node: &MigrateNode,
		params: &Params,
	) -> Result<()> {
		let columns = execute_migrate(self, services, tx, node.clone(), params)?;
		self.stack.push(Variable::columns(columns));
		Ok(())
	}

	pub(crate) fn exec_rollback_migration(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		node: &RollbackMigrationNode,
		params: &Params,
	) -> Result<()> {
		let columns = execute_rollback_migration(self, services, tx, node.clone(), params)?;
		self.stack.push(Variable::columns(columns));
		Ok(())
	}

	pub(crate) fn exec_assert_block(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		node: &AssertBlockNode,
		params: &Params,
	) -> Result<()> {
		let rql = &node.rql;
		let compile_result = services.compiler.compile(tx, rql);

		if node.expect_error {
			// ASSERT ERROR: success if compilation or execution errors
			match compile_result {
				Err(e) => {
					// Compilation error -> assertion passes, push diagnostic
					self.stack.push(Variable::columns(diagnostic_to_columns(&e.0)));
				}
				Ok(CompilationResult::Ready(units)) => {
					let mut caught_diagnostic = None;
					for unit in units.iter() {
						let saved_ip = self.ip;
						self.ip = 0;
						let mut discard = Vec::new();
						let exec_result = self.run(
							services,
							tx,
							&unit.instructions,
							params,
							&mut discard,
						);
						self.ip = saved_ip;
						if let Err(e) = exec_result {
							caught_diagnostic = Some(e.0);
							break;
						}
					}
					if let Some(diag) = caught_diagnostic {
						self.stack.push(Variable::columns(diagnostic_to_columns(&diag)));
					} else {
						let msg = node
							.message
							.as_deref()
							.unwrap_or("expected error but block succeeded");
						return Err(EngineError::AssertionFailed {
							fragment: Fragment::None,
							message: msg.to_string(),
							expression: Some(rql.clone()),
						}
						.into());
					}
				}
				Ok(CompilationResult::Incremental(_)) => {
					return Err(internal_error!(
						"assert block does not support incremental compilation"
					));
				}
			}
		} else {
			// Multi-statement ASSERT: compile body, execute, check last result
			let units = match compile_result {
				Err(e) => return Err(e),
				Ok(CompilationResult::Ready(units)) => units,
				Ok(CompilationResult::Incremental(_)) => {
					return Err(internal_error!(
						"assert block does not support incremental compilation"
					));
				}
			};

			let mut last_error = None;
			for unit in units.iter() {
				let saved_ip = self.ip;
				self.ip = 0;
				let mut discard = Vec::new();
				let exec_result = self.run(services, tx, &unit.instructions, params, &mut discard);
				self.ip = saved_ip;
				if let Err(e) = exec_result {
					last_error = Some(e);
					break;
				}
			}
			if let Some(e) = last_error {
				let msg = node.message.as_deref().unwrap_or("");
				return Err(EngineError::AssertionFailed {
					fragment: Fragment::None,
					message: if msg.is_empty() {
						format!("{}", e)
					} else {
						msg.to_string()
					},
					expression: Some(rql.clone()),
				}
				.into());
			}
		}
		Ok(())
	}
}

/// Convert a `Diagnostic` into a single-row `Columns` with fields:
/// `code`, `message`, `statement`, `label`, `help`.
fn diagnostic_to_columns(diag: &Diagnostic) -> Columns {
	let code_col = Column::new("code", ColumnData::utf8([diag.code.as_str()]));
	let message_col = Column::new("message", ColumnData::utf8([diag.message.as_str()]));
	let statement_col = Column::new(
		"statement",
		match &diag.statement {
			Some(s) => ColumnData::utf8([s.as_str()]),
			None => ColumnData::none_typed(Type::Utf8, 1),
		},
	);
	let label_col = Column::new(
		"label",
		match &diag.label {
			Some(s) => ColumnData::utf8([s.as_str()]),
			None => ColumnData::none_typed(Type::Utf8, 1),
		},
	);
	let help_col = Column::new(
		"help",
		match &diag.help {
			Some(s) => ColumnData::utf8([s.as_str()]),
			None => ColumnData::none_typed(Type::Utf8, 1),
		},
	);
	Columns::new(vec![code_col, message_col, statement_col, label_col, help_col])
}
