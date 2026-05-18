// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_rql::instruction::ScopeType;
use reifydb_type::error::{RuntimeErrorKind, TypeError};

use crate::{
	Result,
	vm::{exec::mask::value_is_truthy, vm::Vm},
};

const MAX_ITERATIONS: usize = 10_000;

impl<'a> Vm<'a> {
	pub(crate) fn exec_jump(&mut self, addr: usize) -> Result<()> {
		self.iteration_count += 1;
		if self.iteration_count > MAX_ITERATIONS {
			return Err(TypeError::Runtime {
				kind: RuntimeErrorKind::MaxIterationsExceeded {
					limit: MAX_ITERATIONS,
				},
				message: format!("Loop exceeded maximum iteration limit of {}", MAX_ITERATIONS),
			}
			.into());
		}
		self.ip = addr;
		Ok(())
	}

	pub(crate) fn exec_jump_if_false_pop(&mut self, addr: usize) -> Result<bool> {
		let value = self.pop_value()?;
		if !value_is_truthy(&value) {
			self.ip = addr;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	pub(crate) fn exec_jump_if_true_pop(&mut self, addr: usize) -> Result<bool> {
		let value = self.pop_value()?;
		if value_is_truthy(&value) {
			self.ip = addr;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	pub(crate) fn exec_enter_scope(&mut self, scope_type: &ScopeType) {
		self.symbols.enter_scope(scope_type.clone());
	}

	pub(crate) fn exec_exit_scope(&mut self) -> Result<()> {
		self.symbols.exit_scope()?;
		Ok(())
	}

	pub(crate) fn exec_break(&mut self, exit_scopes: usize, addr: usize) -> Result<()> {
		for _ in 0..exit_scopes {
			self.symbols.exit_scope()?;
		}
		self.ip = addr;
		Ok(())
	}

	pub(crate) fn exec_continue(&mut self, exit_scopes: usize, addr: usize) -> Result<()> {
		for _ in 0..exit_scopes {
			self.symbols.exit_scope()?;
		}
		self.ip = addr;
		Ok(())
	}
}
