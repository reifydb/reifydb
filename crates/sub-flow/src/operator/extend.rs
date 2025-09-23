// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::FlowChange,
	interface::{Transaction, expression::Expression},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

use crate::Operator;

pub struct ExtendOperator {
	expressions: Vec<Expression<'static>>,
}

impl ExtendOperator {
	pub fn new(expressions: Vec<Expression<'static>>) -> Self {
		Self {
			expressions,
		}
	}
}

impl<T: Transaction> Operator<T> for ExtendOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row extend processing
		// For now, just pass through all changes
		Ok(change)
	}
}
