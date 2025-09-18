// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{flow::FlowChange, interface::Transaction};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::Operator;

/// Apply operator for dynamic invocation of user-defined functions
pub struct ApplyOperator<T: Transaction> {
	operator: Box<dyn Operator<T>>,
}

impl<T: Transaction> ApplyOperator<T> {
	pub fn new(operator: Box<dyn Operator<T>>) -> Self {
		Self {
			operator,
		}
	}
}

impl<T: Transaction> Operator<T> for ApplyOperator<T> {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		self.operator.apply(txn, change, evaluator)
	}
}
