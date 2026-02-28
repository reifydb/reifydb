// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::key::EncodedKey,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	internal,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::columns::Columns,
};
use reifydb_type::{Result, error::Error, value::row_number::RowNumber};

use crate::{
	operator::{Operator, Operators, stateful::row::RowNumberProvider},
	transaction::FlowTransaction,
};

/// APPEND operator that appends N input flows (N >= 2) with identical schemas
/// into a single output flow. Keeps all rows including duplicates.
pub struct AppendOperator {
	node: FlowNodeId,
	/// Parent operators indexed by their position (0..N)
	parents: Vec<Arc<Operators>>,
	/// Input node IDs for matching ChangeOrigin
	input_nodes: Vec<FlowNodeId>,
	/// Row number provider for stable output row numbers
	row_number_provider: RowNumberProvider,
}

impl AppendOperator {
	pub fn new(node: FlowNodeId, parents: Vec<Arc<Operators>>, input_nodes: Vec<FlowNodeId>) -> Self {
		debug_assert_eq!(parents.len(), input_nodes.len());
		debug_assert!(parents.len() >= 2, "Append requires at least 2 inputs");

		Self {
			node,
			parents,
			input_nodes,
			row_number_provider: RowNumberProvider::new(node),
		}
	}

	/// Find which parent index a change originated from
	fn determine_parent_index(&self, change: &Change) -> Option<usize> {
		match &change.origin {
			ChangeOrigin::Flow(from_node) => self.input_nodes.iter().position(|n| n == from_node),
			ChangeOrigin::Primitive(_) => None,
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

impl Operator for AppendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let parent_index = self.determine_parent_index(&change).ok_or_else(|| {
			Error(internal!("Append received change from unknown node: {:?}", change.origin))
		})?;

		let mut result_diffs = Vec::with_capacity(change.diffs.len());

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => {
					let row_count = post.row_count();
					if row_count == 0 {
						continue;
					}

					let mut output_row_numbers = Vec::with_capacity(row_count);
					for row_idx in 0..row_count {
						let source_row_number = post.row_numbers[row_idx];
						let composite_key =
							Self::make_composite_key(parent_index as u8, source_row_number);
						let (output_row_number, _is_new) = self
							.row_number_provider
							.get_or_create_row_number(txn, &composite_key)?;

						output_row_numbers.push(output_row_number);
					}

					let output = Columns::with_row_numbers(
						post.columns.as_ref().to_vec(),
						output_row_numbers,
					);

					result_diffs.push(Diff::Insert {
						post: output,
					});
				}
				Diff::Update {
					pre,
					post,
				} => {
					let row_count = post.row_count();
					if row_count == 0 {
						continue;
					}

					let mut output_row_numbers = Vec::with_capacity(row_count);
					for row_idx in 0..row_count {
						let source_row_number = pre.row_numbers[row_idx];
						let composite_key =
							Self::make_composite_key(parent_index as u8, source_row_number);
						let (output_row_number, _) = self
							.row_number_provider
							.get_or_create_row_number(txn, &composite_key)?;
						output_row_numbers.push(output_row_number);
					}

					let pre_output = Columns::with_row_numbers(
						pre.columns.as_ref().to_vec(),
						output_row_numbers.clone(),
					);
					let post_output = Columns::with_row_numbers(
						post.columns.as_ref().to_vec(),
						output_row_numbers,
					);

					result_diffs.push(Diff::Update {
						pre: pre_output,
						post: post_output,
					});
				}
				Diff::Remove {
					pre,
				} => {
					let row_count = pre.row_count();
					if row_count == 0 {
						continue;
					}

					let mut output_row_numbers = Vec::with_capacity(row_count);
					for row_idx in 0..row_count {
						let source_row_number = pre.row_numbers[row_idx];
						let composite_key =
							Self::make_composite_key(parent_index as u8, source_row_number);
						let (output_row_number, _) = self
							.row_number_provider
							.get_or_create_row_number(txn, &composite_key)?;
						output_row_numbers.push(output_row_number);
					}

					let output = Columns::with_row_numbers(
						pre.columns.as_ref().to_vec(),
						output_row_numbers,
					);

					result_diffs.push(Diff::Remove {
						pre: output,
					});
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result_diffs))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		let mut found_columns: Vec<Columns> = Vec::new();

		for &row_number in rows {
			// Reverse lookup: output row number -> composite key
			let Some(key) = self.row_number_provider.get_key_for_row_number(txn, row_number)? else {
				continue;
			};

			let Some((parent_index, source_row_number)) = Self::parse_composite_key(key.as_ref()) else {
				continue;
			};

			if parent_index >= self.parents.len() {
				continue;
			}

			let parent_cols = self.parents[parent_index].pull(txn, &[source_row_number])?;

			if !parent_cols.is_empty() {
				// Replace row number with append output row number
				let updated = Columns::with_row_numbers(
					parent_cols.columns.as_ref().to_vec(),
					vec![row_number],
				);
				found_columns.push(updated);
			}
		}

		// Combine found rows
		if found_columns.is_empty() {
			self.parents[0].pull(txn, &[])
		} else if found_columns.len() == 1 {
			Ok(found_columns.remove(0))
		} else {
			let mut result = found_columns.remove(0);
			for cols in found_columns {
				result.row_numbers.make_mut().extend(cols.row_numbers.iter().copied());
				for (i, col) in cols.columns.into_iter().enumerate() {
					result.columns.make_mut()[i]
						.extend(col)
						.expect("schema mismatch in append pull");
				}
			}
			Ok(result)
		}
	}
}
