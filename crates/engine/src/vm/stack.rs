// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::internal;
use reifydb_evaluate::stack::Variable;
#[cfg(reifydb_assertions)]
use reifydb_value::value::canonical::assert_canonical_floats;
use reifydb_value::{error, reifydb_assertions};

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
		reifydb_assertions! {
			match &value {
				Variable::Columns {
					batch,
				}
				| Variable::ForIterator {
					batch,
					..
				} => assert_canonical_floats(batch, "stack push"),
				Variable::Closure(_) => {}
			}
		}
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
	Return(Option<RecordBatch>),
}

impl ControlFlow {
	pub fn is_normal(&self) -> bool {
		matches!(self, ControlFlow::Normal)
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, Float64Array, RecordBatch};
	use reifydb_evaluate::stack::Variable;

	use super::Stack;

	fn negative_zero_batch() -> RecordBatch {
		let column: ArrayRef = Arc::new(Float64Array::from(vec![-0.0f64]));
		RecordBatch::try_from_iter([("c", column)]).unwrap()
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is not canonical")]
	fn test_push_columns_with_negative_zero_panics() {
		// Every VM value goes through push, so a raw -0.0 must stop here or a later compare splits zero.
		Stack::new().push(Variable::Columns {
			batch: negative_zero_batch(),
		});
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is not canonical")]
	fn test_push_for_iterator_with_negative_zero_panics() {
		// A for loop's batch is pushed as its own variant, so the check must cover it too or -0.0 slips past.
		Stack::new().push(Variable::ForIterator {
			batch: negative_zero_batch(),
			index: 0,
		});
	}
}
