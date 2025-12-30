// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

//! Thread-local operator context stack for error diagnostics.
//!
//! This module provides a mechanism to track the current operator call chain
//! during flow execution. When an error occurs, the current chain can be
//! captured and included in the error diagnostic.

use std::cell::RefCell;

use reifydb_core::interface::FlowNodeId;
use reifydb_type::diagnostic::OperatorChainEntry;

/// Single entry in the operator call chain.
#[derive(Debug, Clone)]
pub struct OperatorContextEntry {
	pub node_id: FlowNodeId,
	pub operator_name: &'static str,
	pub operator_version: &'static str,
}

thread_local! {
	static OPERATOR_STACK: RefCell<Vec<OperatorContextEntry>> = const { RefCell::new(Vec::new()) };
}

/// Push an operator context onto the stack.
pub fn push_operator_context(entry: OperatorContextEntry) {
	OPERATOR_STACK.with(|stack| {
		stack.borrow_mut().push(entry);
	});
}

/// Pop the current operator context from the stack.
pub fn pop_operator_context() {
	OPERATOR_STACK.with(|stack| {
		stack.borrow_mut().pop();
	});
}

/// Capture the current operator call chain as a vector of OperatorChainEntry.
///
/// This is used when creating error diagnostics to include the full
/// operator hierarchy that led to the error.
pub fn capture_operator_chain() -> Vec<OperatorChainEntry> {
	OPERATOR_STACK.with(|stack| {
		stack.borrow()
			.iter()
			.map(|e| OperatorChainEntry {
				node_id: e.node_id.0,
				operator_name: e.operator_name.to_string(),
				operator_version: e.operator_version.to_string(),
			})
			.collect()
	})
}

/// RAII guard for operator context.
///
/// Automatically pushes the operator context when created and pops it when dropped.
/// This ensures the context stack is always balanced, even if the operator
/// returns early due to an error.
pub struct OperatorContextGuard {
	_private: (),
}

impl OperatorContextGuard {
	/// Create a new guard that pushes the operator context.
	pub fn new(entry: OperatorContextEntry) -> Self {
		push_operator_context(entry);
		Self {
			_private: (),
		}
	}
}

impl Drop for OperatorContextGuard {
	fn drop(&mut self) {
		pop_operator_context();
	}
}

/// Macro to create a context guard for an operator.
///
/// Usage:
/// ```ignore
/// let _guard = operator_context_guard!(self);
/// ```
#[macro_export]
macro_rules! operator_context_guard {
	($op:expr) => {{
		use $crate::operator::{
			context::{OperatorContextEntry, OperatorContextGuard},
			info::OperatorInfo,
		};
		OperatorContextGuard::new(OperatorContextEntry {
			node_id: $op.operator_id(),
			operator_name: $op.operator_name(),
			operator_version: $op.operator_version(),
		})
	}};
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_push_pop_context() {
		let entry = OperatorContextEntry {
			node_id: FlowNodeId(1),
			operator_name: "TestOp",
			operator_version: "1.0.0",
		};

		push_operator_context(entry);
		let chain = capture_operator_chain();
		assert_eq!(chain.len(), 1);
		assert_eq!(chain[0].operator_name, "TestOp");

		pop_operator_context();
		let chain = capture_operator_chain();
		assert_eq!(chain.len(), 0);
	}

	#[test]
	fn test_context_guard() {
		{
			let entry = OperatorContextEntry {
				node_id: FlowNodeId(2),
				operator_name: "GuardedOp",
				operator_version: "2.0.0",
			};
			let _guard = OperatorContextGuard::new(entry);

			let chain = capture_operator_chain();
			assert_eq!(chain.len(), 1);
			assert_eq!(chain[0].operator_name, "GuardedOp");
		}

		// Guard is dropped, context should be popped
		let chain = capture_operator_chain();
		assert_eq!(chain.len(), 0);
	}

	#[test]
	fn test_nested_context() {
		let entry1 = OperatorContextEntry {
			node_id: FlowNodeId(1),
			operator_name: "Outer",
			operator_version: "1.0.0",
		};
		let entry2 = OperatorContextEntry {
			node_id: FlowNodeId(2),
			operator_name: "Inner",
			operator_version: "1.0.0",
		};

		let _guard1 = OperatorContextGuard::new(entry1);
		{
			let _guard2 = OperatorContextGuard::new(entry2);

			let chain = capture_operator_chain();
			assert_eq!(chain.len(), 2);
			assert_eq!(chain[0].operator_name, "Outer");
			assert_eq!(chain[1].operator_name, "Inner");
		}

		let chain = capture_operator_chain();
		assert_eq!(chain.len(), 1);
		assert_eq!(chain[0].operator_name, "Outer");
	}
}
