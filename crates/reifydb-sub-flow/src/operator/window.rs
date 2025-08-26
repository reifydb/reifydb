// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::BTreeMap;

use reifydb_core::{
	CowVec, Value,
	flow::{FlowChange, FlowDiff},
	interface::{CommandTransaction, Evaluator, expression::Expression},
	row::EncodedKey,
	value::columnar::Columns,
};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{Operator, OperatorContext},
};

// Key for storing window state
#[derive(Debug, Clone)]
struct FlowWindowStateKey {
	flow_id: u64,
	node_id: u64,
	partition_key: Vec<u8>,
	timestamp: i64,
	row_id: u64,
}

impl FlowWindowStateKey {
	const KEY_PREFIX: u8 = 0xF5;

	fn new(
		flow_id: u64,
		node_id: u64,
		partition_key: Vec<Value>,
		timestamp: i64,
		row_id: u64,
	) -> Self {
		let serialized =
			bincode::serialize(&partition_key).unwrap_or_default();
		Self {
			flow_id,
			node_id,
			partition_key: serialized,
			timestamp,
			row_id,
		}
	}

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		key.extend(&(self.partition_key.len() as u32).to_be_bytes());
		key.extend(&self.partition_key);
		key.extend(&self.timestamp.to_be_bytes());
		key.extend(&self.row_id.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowType {
	// Sliding time window
	TimeSliding {
		duration_ms: i64,
	},
	// Tumbling time window
	TimeTumbling {
		duration_ms: i64,
	},
	// Row-based sliding window
	RowSliding {
		size: usize,
	},
	// Session window
	Session {
		gap_ms: i64,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowState {
	// Rows in the current window, keyed by timestamp
	rows: BTreeMap<i64, Vec<(u64, Columns)>>,
	// Aggregate state for the window
	aggregate_state: Option<Vec<Value>>,
}

pub struct WindowOperator {
	flow_id: u64,
	node_id: u64,
	window_type: WindowType,
	partition_keys: Vec<Expression>,
	order_key: Option<Expression>, // Usually a timestamp
	aggregate_exprs: Vec<Expression>,
}

impl WindowOperator {
	pub fn new(
		flow_id: u64,
		node_id: u64,
		window_type: WindowType,
		partition_keys: Vec<Expression>,
		order_key: Option<Expression>,
		aggregate_exprs: Vec<Expression>,
	) -> Self {
		Self {
			flow_id,
			node_id,
			window_type,
			partition_keys,
			order_key,
			aggregate_exprs,
		}
	}

	fn get_timestamp(&self, _columns: &Columns, _row_idx: usize) -> i64 {
		// Extract timestamp from order_key expression
		// For now, use current time
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as i64
	}

	fn compute_window_bounds(&self, timestamp: i64) -> (i64, i64) {
		match &self.window_type {
			WindowType::TimeSliding {
				duration_ms,
			} => (timestamp - duration_ms, timestamp),
			WindowType::TimeTumbling {
				duration_ms,
			} => {
				let window_start =
					(timestamp / duration_ms) * duration_ms;
				(window_start, window_start + duration_ms)
			}
			_ => (timestamp, timestamp), // Simplified
		}
	}
}

impl<E: Evaluator> Operator<E> for WindowOperator {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> Result<FlowChange> {
		let mut output_diffs = Vec::new();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					row_ids,
					after,
				} => {
					for (idx, &row_id) in
						row_ids.iter().enumerate()
					{
						let timestamp = self
							.get_timestamp(
								after, idx,
							);
						let (window_start, window_end) = self.compute_window_bounds(timestamp);

						// For time windows, we need to:
						// 1. Add this row to the window
						//    state
						// 2. Remove any rows that are
						//    now outside the window
						// 3. Recompute aggregates
						// 4. Emit the new window result

						// Simplified: just emit the row
						// with window bounds metadata
						let window_columns =
							after.clone();

						// Add window metadata columns
						// (window_start, window_end)
						// This is simplified - real
						// implementation would compute
						// aggregates

						output_diffs
							.push(FlowDiff::Insert {
							source: *source,
							row_ids: vec![row_id],
							after: window_columns,
						});
					}
				}

				FlowDiff::Remove {
					source,
					row_ids,
					before,
				} => {
					// For removes, we need to:
					// 1. Remove from window state
					// 2. Recompute aggregates
					// 3. Emit updated window results

					// Simplified pass-through
					output_diffs.push(FlowDiff::Remove {
						source: *source,
						row_ids: row_ids.clone(),
						before: before.clone(),
					});
				}

				FlowDiff::Update {
					..
				} => {
					// Handle as remove + insert
					output_diffs.push(diff.clone());
				}
			}
		}

		Ok(FlowChange {
			diffs: output_diffs,
			metadata: change.metadata.clone(),
		})
	}
}
