// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{flow::FlowChange, interface::CommandTransaction};
use reifydb_engine::StandardEvaluator;

use crate::{Result, operator::Operator};

pub struct UnionOperator {
	// Union doesn't need state - it just passes through all changes
}

impl UnionOperator {
	pub fn new() -> Self {
		Self {}
	}
}

impl<T: CommandTransaction> Operator<T> for UnionOperator {
	fn apply(
		&self,
		txn: &mut T,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> Result<FlowChange> {
		// Union is a simple pass-through operator
		// It combines inputs from multiple sources
		// The FlowEngine handles routing multiple inputs to this
		// operator
		Ok(change.clone())
	}
}
