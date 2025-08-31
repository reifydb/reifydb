// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, JoinType, Value,
	flow::FlowChange,
	interface::{CommandTransaction, Evaluator, expression::Expression},
	row::EncodedKey,
};

use crate::{
	Result,
	operator::{Operator, OperatorContext},
};

// Key for storing join state
#[derive(Debug, Clone)]
struct FlowJoinStateKey {
	flow_id: u64,
	node_id: u64,
	side: u8,
	join_key: Vec<u8>,
	row_id: u64,
}

impl FlowJoinStateKey {
	const KEY_PREFIX: u8 = 0xF1;

	fn new(
		flow_id: u64,
		node_id: u64,
		side: u8,
		join_key: Vec<Value>,
		row_id: u64,
	) -> Self {
		let serialized =
			serde_json::to_vec(&join_key).unwrap_or_default();
		Self {
			flow_id,
			node_id,
			side,
			join_key: serialized,
			row_id,
		}
	}

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		key.push(self.side);
		key.extend(&(self.join_key.len() as u32).to_be_bytes());
		key.extend(&self.join_key);
		key.extend(&self.row_id.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}
}

pub struct JoinOperator {
	join_type: JoinType,
	left_keys: Vec<Expression<'static>>,
	right_keys: Vec<Expression<'static>>,
}

impl JoinOperator {
	pub fn new(
		join_type: JoinType,
		left_keys: Vec<Expression<'static>>,
		right_keys: Vec<Expression<'static>>,
	) -> Self {
		Self {
			join_type,
			left_keys,
			right_keys,
		}
	}
}

impl<E: Evaluator> Operator<E> for JoinOperator {
	fn apply<T: CommandTransaction>(
		&self,
		_ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> Result<FlowChange> {
		// For now, return a simple pass-through
		// Full implementation would handle join logic here
		Ok(change.clone())
	}
}
