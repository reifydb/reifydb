// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey,
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, FlowNodeId, Params, Transaction, expression::Expression},
	row::EncodedRow,
	util::CowVec,
	value::columnar::Columns,
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_type::{Error, Value, internal_error};
use serde::{Deserialize, Serialize};

use crate::operator::{Operator, transform::TransformOperator};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctEntry {
	count: usize,
	first_row_id: u64,
	row_data: Vec<Value>,
}

pub struct DistinctOperator {
	node: FlowNodeId,
	expressions: Vec<Expression<'static>>,
}

impl DistinctOperator {
	pub fn new(node: FlowNodeId, expressions: Vec<Expression<'static>>) -> Self {
		Self {
			node,
			expressions,
		}
	}

	fn hash_to_key(hash: Hash128) -> EncodedKey {
		let mut key = Vec::with_capacity(16);
		key.extend(&hash.0.to_be_bytes());
		EncodedKey::new(key)
	}

	fn hash_row_with_expressions(
		evaluator: &StandardEvaluator,
		expressions: &[Expression],
		columns: &Columns,
		row_idx: usize,
	) -> crate::Result<Hash128> {
		let mut buffer = Vec::new();

		if expressions.is_empty() {
			// If no expressions specified, hash all columns
			for col in columns.iter() {
				let value = col.data().get_value(row_idx);
				buffer.extend(format!("{:?}", value).as_bytes());
			}
		} else {
			// Hash only the specified expressions
			let row_count = columns.row_count();
			let empty_params = Params::None;
			let eval_ctx = EvaluationContext {
				target_column: None,
				column_policies: Vec::new(),
				columns: columns.clone(),
				row_count,
				take: None,
				params: &empty_params,
			};
			for expr in expressions {
				let result = evaluator.evaluate(&eval_ctx, expr)?;
				let value = result.data().get_value(row_idx);
				buffer.extend(format!("{:?}", value).as_bytes());
			}
		}

		Ok(xxh3_128(&buffer))
	}

	fn extract_row_values(
		evaluator: &StandardEvaluator,
		expressions: &[Expression],
		columns: &Columns,
		row_idx: usize,
	) -> crate::Result<Vec<Value>> {
		if expressions.is_empty() {
			// If no expressions specified, extract all columns
			Ok(columns.iter().map(|col| col.data().get_value(row_idx)).collect())
		} else {
			// Extract only the specified expressions
			let row_count = columns.row_count();
			let empty_params = Params::None;
			let eval_ctx = EvaluationContext {
				target_column: None,
				column_policies: Vec::new(),
				columns: columns.clone(),
				row_count,
				take: None,
				params: &empty_params,
			};
			let mut values = Vec::new();
			for expr in expressions {
				let result = evaluator.evaluate(&eval_ctx, expr)?;
				values.push(result.data().get_value(row_idx));
			}
			Ok(values)
		}
	}
}

impl<T: Transaction> Operator<T> for DistinctOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let mut output_diffs = Vec::new();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					row_ids,
					post: after,
				} => {
					let mut new_distinct_rows = Vec::new();

					for (idx, &row_id) in row_ids.iter().enumerate() {
						let row_hash = Self::hash_row_with_expressions(
							evaluator,
							&self.expressions,
							after,
							idx,
						)?;
						let key = Self::hash_to_key(row_hash);

						// Check if we've seen this row
						// before
						let existing = self.get(txn, &key).ok();

						if existing.as_ref().map(|r| r.as_ref().is_empty()).unwrap_or(true) {
							// First time seeing
							// this distinct value
							let entry = DistinctEntry {
								count: 1,
								first_row_id: row_id.0,
								row_data: Self::extract_row_values(
									evaluator,
									&self.expressions,
									after,
									idx,
								)?,
							};

							let serialized = serde_json::to_vec(&entry).map_err(|e| {
								Error(internal_error!("Failed to serialize: {}", e))
							})?;
							self.set(txn, &key, EncodedRow(CowVec::new(serialized)))?;

							// Emit this row as new
							// distinct value
							new_distinct_rows.push(row_id);

							// Add columns for this
							// row - simplified,
							// just clone the row
							// In production, we'd
							// properly handle
							// column slicing
						} else {
							// Update the count for
							// existing distinct
							// value
							let bytes = existing.unwrap();
							let mut entry: DistinctEntry = serde_json::from_slice(
								bytes.as_ref(),
							)
							.map_err(|e| {
								Error(internal_error!("Failed to deserialize: {}", e))
							})?;
							entry.count += 1;
							let serialized = serde_json::to_vec(&entry).map_err(|e| {
								Error(internal_error!("Failed to serialize: {}", e))
							})?;
							self.set(txn, &key, EncodedRow(CowVec::new(serialized)))?;
							// Don't emit since it's
							// not distinct
						}
					}

					if !new_distinct_rows.is_empty() {
						// For simplicity, just pass
						// through the unique rows
						// A real implementation would
						// properly handle columnar data
						output_diffs.push(FlowDiff::Insert {
							source: *source,
							row_ids: new_distinct_rows,
							post: after.clone(),
						});
					}
				}

				FlowDiff::Remove {
					source,
					row_ids,
					pre: before,
				} => {
					let mut removed_distinct_rows = Vec::new();

					for (idx, &row_id) in row_ids.iter().enumerate() {
						let row_hash = Self::hash_row_with_expressions(
							evaluator,
							&self.expressions,
							before,
							idx,
						)?;
						let key = Self::hash_to_key(row_hash);

						let existing = self.get(txn, &key).ok();
						if let Some(data) = existing {
							if !data.as_ref().is_empty() {
								let mut entry: DistinctEntry = serde_json::from_slice(
									data.as_ref(),
								)
								.map_err(|e| {
									Error(internal_error!(
										"Failed to deserialize: {}",
										e
									))
								})?;

								if entry.count > 1 {
									// Still
									// have
									// other
									// instances
									entry.count -= 1;
									let serialized = serde_json::to_vec(&entry)
										.map_err(|e| {
											Error(internal_error!(
												"Failed to serialize: {}",
												e
											))
										})?;
									self.set(
										txn,
										&key,
										EncodedRow(CowVec::new(serialized)),
									)?;
								} else {
									// Last instance
									// - remove from
									// state
									// and emit
									// retraction
									self.remove(txn, &key)?;

									removed_distinct_rows.push(
										reifydb_type::RowNumber(
											entry.first_row_id,
										),
									);
								}
							}
						}
					}

					if !removed_distinct_rows.is_empty() {
						output_diffs.push(FlowDiff::Remove {
							source: *source,
							row_ids: removed_distinct_rows,
							pre: before.clone(),
						});
					}
				}

				FlowDiff::Update {
					source,
					row_ids,
					pre: before,
					post: after,
				} => {
					// Handle update as remove + insert
					// First process the remove
					let remove_diff = FlowDiff::Remove {
						source: *source,
						row_ids: row_ids.clone(),
						pre: before.clone(),
					};
					let remove_change = FlowChange::new(vec![remove_diff]);
					let remove_result = self.apply(txn, &remove_change, evaluator)?;
					output_diffs.extend(remove_result.diffs);

					// Then process the insert
					let insert_diff = FlowDiff::Insert {
						source: *source,
						row_ids: row_ids.clone(),
						post: after.clone(),
					};
					let insert_change = FlowChange::new(vec![insert_diff]);
					let insert_result = self.apply(txn, &insert_change, evaluator)?;
					output_diffs.extend(insert_result.diffs);
				}
			}
		}

		Ok(FlowChange {
			diffs: output_diffs,
			metadata: change.metadata.clone(),
		})
	}
}

impl<T: Transaction> TransformOperator<T> for DistinctOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}
}
