// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, SortKey, Value,
	flow::FlowChange,
	interface::{CommandTransaction, Evaluator},
	row::EncodedKey,
	value::columnar::Columns,
};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{Operator, OperatorContext},
};

// Key for storing sorted state
#[derive(Debug, Clone)]
struct FlowSortStateKey {
	flow_id: u64,
	node_id: u64,
	sort_index: u64,
}

impl FlowSortStateKey {
	const KEY_PREFIX: u8 = 0xF2;

	fn new(flow_id: u64, node_id: u64, sort_index: u64) -> Self {
		Self {
			flow_id,
			node_id,
			sort_index,
		}
	}

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		key.extend(&self.sort_index.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SortedState {
	rows: Vec<(Vec<Value>, Columns)>,
	total_rows: usize,
}

pub struct SortOperator {
	sort_keys: Vec<SortKey>,
}

impl SortOperator {
	pub fn new(sort_keys: Vec<SortKey>) -> Self {
		Self {
			sort_keys,
		}
	}
}

impl<E: Evaluator> Operator<E> for SortOperator {
	fn apply<T: CommandTransaction>(
		&self,
		_ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> Result<FlowChange> {
		// For incremental updates, we would:
		// 1. Load current sorted state
		// 2. Apply inserts/deletes to maintain sorted order
		// 3. Emit changes at affected positions
		// For now, simplified pass-through
		Ok(change.clone())
	}
}
