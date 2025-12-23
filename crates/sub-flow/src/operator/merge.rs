// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{EncodedKey, Error, Row, interface::FlowNodeId, util::encoding::keycode::KeySerializer};
use reifydb_engine::StandardRowEvaluator;
use reifydb_flow_operator_sdk::{FlowChange, FlowChangeOrigin, FlowDiff};
use reifydb_type::{RowNumber, internal};

use crate::{
	operator::{Operator, Operators, stateful::RowNumberProvider},
	transaction::FlowTransaction,
};

/// MERGE operator that merges N input flows (N >= 2) with identical schemas
/// into a single output flow. Keeps all rows including duplicates.
pub struct MergeOperator {
	node: FlowNodeId,
	/// Parent operators indexed by their position (0..N)
	parents: Vec<Arc<Operators>>,
	/// Input node IDs for matching FlowChangeOrigin
	input_nodes: Vec<FlowNodeId>,
	/// Row number provider for stable output row numbers
	row_number_provider: RowNumberProvider,
}

impl MergeOperator {
	pub fn new(node: FlowNodeId, parents: Vec<Arc<Operators>>, input_nodes: Vec<FlowNodeId>) -> Self {
		debug_assert_eq!(parents.len(), input_nodes.len());
		debug_assert!(parents.len() >= 2, "Merge requires at least 2 inputs");

		Self {
			node,
			parents,
			input_nodes,
			row_number_provider: RowNumberProvider::new(node),
		}
	}

	/// Find which parent index a change originated from
	fn determine_parent_index(&self, change: &FlowChange) -> Option<usize> {
		match &change.origin {
			FlowChangeOrigin::Internal(from_node) => self.input_nodes.iter().position(|n| n == from_node),
			FlowChangeOrigin::External(_) => None,
		}
	}

	/// Create composite key: [parent_index: u8][source_row_number: u64]
	fn make_composite_key(parent_index: u8, source_row: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(parent_index);
		serializer.extend_u64(source_row.0);
		EncodedKey::new(serializer.finish())
	}

	/// Parse composite key to extract (parent_index, source_row_number)
	/// Key format after keycode encoding: [!parent_index: 1 byte][!source_row_number: 8 bytes]
	fn parse_composite_key(key_bytes: &[u8]) -> Option<(usize, RowNumber)> {
		if key_bytes.len() < 9 {
			return None;
		}
		// Decode parent_index (u8 with bitwise NOT)
		let parent_index = !key_bytes[0];
		// Decode source_row_number (u64 big-endian with bitwise NOT)
		let source_row = u64::from_be_bytes([
			!key_bytes[1],
			!key_bytes[2],
			!key_bytes[3],
			!key_bytes[4],
			!key_bytes[5],
			!key_bytes[6],
			!key_bytes[7],
			!key_bytes[8],
		]);
		Some((parent_index as usize, RowNumber(source_row)))
	}
}

#[async_trait]
impl Operator for MergeOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// Determine which parent this change came from
		let parent_index = self.determine_parent_index(&change).ok_or_else(|| {
			Error(internal!("Merge received change from unknown node: {:?}", change.origin))
		})?;

		let mut result_diffs = Vec::with_capacity(change.diffs.len());

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Get or create stable row number for this source row
					let composite_key = Self::make_composite_key(parent_index as u8, post.number);
					let (output_row_number, _is_new) = self
						.row_number_provider
						.get_or_create_row_number(txn, &composite_key)
						.await?;

					result_diffs.push(FlowDiff::Insert {
						post: Row {
							number: output_row_number,
							encoded: post.encoded,
							layout: post.layout,
						},
					});
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Row number should already exist from insert
					let composite_key = Self::make_composite_key(parent_index as u8, pre.number);
					let (output_row_number, _) = self
						.row_number_provider
						.get_or_create_row_number(txn, &composite_key)
						.await?;

					result_diffs.push(FlowDiff::Update {
						pre: Row {
							number: output_row_number,
							encoded: pre.encoded,
							layout: pre.layout,
						},
						post: Row {
							number: output_row_number,
							encoded: post.encoded,
							layout: post.layout,
						},
					});
				}
				FlowDiff::Remove {
					pre,
				} => {
					let composite_key = Self::make_composite_key(parent_index as u8, pre.number);
					let (output_row_number, _) = self
						.row_number_provider
						.get_or_create_row_number(txn, &composite_key)
						.await?;

					result_diffs.push(FlowDiff::Remove {
						pre: Row {
							number: output_row_number,
							encoded: pre.encoded,
							layout: pre.layout,
						},
					});
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, result_diffs))
	}

	async fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		let mut result = Vec::with_capacity(rows.len());

		for &row_number in rows {
			// Reverse lookup: output row number -> composite key
			let Some(key) = self.row_number_provider.get_key_for_row_number(txn, row_number).await? else {
				result.push(None);
				continue;
			};

			// Parse composite key to get parent index and source row number
			let Some((parent_index, source_row_number)) = Self::parse_composite_key(key.as_ref()) else {
				result.push(None);
				continue;
			};

			// Validate parent index
			if parent_index >= self.parents.len() {
				result.push(None);
				continue;
			}

			// Delegate to parent operator
			let parent_rows = self.parents[parent_index].get_rows(txn, &[source_row_number]).await?;

			if let Some(Some(mut row)) = parent_rows.into_iter().next() {
				// Replace row number with merge output row number
				row.number = row_number;
				result.push(Some(row));
			} else {
				result.push(None);
			}
		}

		Ok(result)
	}
}
