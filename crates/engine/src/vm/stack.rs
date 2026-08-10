// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{internal, value::column::columns::Columns};
use reifydb_evaluate::stack::Variable;
use reifydb_value::error;

use crate::Result;

#[derive(Debug, Clone)]
pub struct Stack {
	variables: Vec<Variable>,
}

impl Stack {
	pub fn new() -> Self {
		Self {
			variables: Vec::new(),
		}
	}

	pub fn push(&mut self, value: Variable) {
		self.variables.push(value);
	}

	pub fn pop(&mut self) -> Result<Variable> {
		self.variables.pop().ok_or_else(|| error!(internal!("VM data stack underflow")))
	}

	pub fn peek(&self) -> Option<&Variable> {
		self.variables.last()
	}

	pub fn is_empty(&self) -> bool {
		self.variables.is_empty()
	}

	pub fn len(&self) -> usize {
		self.variables.len()
	}
}

impl Default for Stack {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Debug, Clone)]
pub enum ControlFlow {
	Normal,
	Break,
	Continue,
	Return(Option<Columns>),
}

impl ControlFlow {
	pub fn is_normal(&self) -> bool {
		matches!(self, ControlFlow::Normal)
	}
}
