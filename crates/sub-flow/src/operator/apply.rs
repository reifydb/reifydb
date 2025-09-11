// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{flow::FlowChange, interface::CommandTransaction};
use reifydb_engine::StandardEvaluator;

use crate::operator::Operator;

/// Apply operator for dynamic invocation of user-defined functions
pub struct ApplyOperator<T: CommandTransaction> {
	operator: Box<dyn Operator<T>>,
}

impl<T: CommandTransaction> ApplyOperator<T> {
	pub fn new(operator: Box<dyn Operator<T>>) -> Self {
		Self {
			operator,
		}
	}
}

impl<T: CommandTransaction> Operator<T> for ApplyOperator<T> {
	fn apply(
		&self,
		txn: &mut T,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		self.operator.apply(txn, change, evaluator)
	}
}
