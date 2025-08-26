// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, Value,
	flow::{FlowChange, FlowDiff},
	interface::{CommandTransaction, Evaluator},
	row::EncodedKey,
	value::columnar::Columns,
};
use serde::{Deserialize, Serialize};

use crate::{
	Result,
	operator::{Operator, OperatorContext},
};

// Key for storing distinct state
#[derive(Debug, Clone)]
struct FlowDistinctStateKey {
	flow_id: u64,
	node_id: u64,
	row_hash: u64,
}

impl FlowDistinctStateKey {
	const KEY_PREFIX: u8 = 0xF4;

	fn new(flow_id: u64, node_id: u64, row_hash: u64) -> Self {
		Self {
			flow_id,
			node_id,
			row_hash,
		}
	}

	fn encode(&self) -> EncodedKey {
		let mut key = Vec::new();
		key.push(Self::KEY_PREFIX);
		key.extend(&self.flow_id.to_be_bytes());
		key.extend(&self.node_id.to_be_bytes());
		key.extend(&self.row_hash.to_be_bytes());
		EncodedKey(CowVec::new(key))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctEntry {
	count: usize,
	first_row_id: u64,
	row_data: Vec<Value>,
}

pub struct DistinctOperator {
	flow_id: u64,
	node_id: u64,
}

impl DistinctOperator {
	pub fn new(flow_id: u64, node_id: u64) -> Self {
		Self {
			flow_id,
			node_id,
		}
	}

	fn hash_row(columns: &Columns, row_idx: usize) -> u64 {
		use std::{
			collections::hash_map::DefaultHasher,
			hash::{Hash, Hasher},
		};

		let mut hasher = DefaultHasher::new();
		for col in columns.iter() {
			let value = col.data().get_value(row_idx);
			format!("{:?}", value).hash(&mut hasher);
		}
		hasher.finish()
	}

	fn extract_row_values(columns: &Columns, row_idx: usize) -> Vec<Value> {
		columns.iter()
			.map(|col| col.data().get_value(row_idx))
			.collect()
	}
}

impl<E: Evaluator> Operator<E> for DistinctOperator {
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
					let mut new_distinct_rows = Vec::new();

					for (idx, &row_id) in
						row_ids.iter().enumerate()
					{
						let row_hash = Self::hash_row(
							after, idx,
						);
						let key = FlowDistinctStateKey::new(
                            self.flow_id,
                            self.node_id,
                            row_hash,
                        );

						// Check if we've seen this row
						// before
						let existing = ctx
							.txn
							.get(&key.encode())?;

						if existing.is_none() {
							// First time seeing
							// this distinct value
							let entry = DistinctEntry {
                                count: 1,
                                first_row_id: row_id.0,
                                row_data: Self::extract_row_values(after, idx),
                            };

							let serialized = bincode::serialize(&entry)
                                .map_err(|e| reifydb_core::Error(reifydb_core::internal_error!(
                                    "Failed to serialize: {}", e
                                )))?;
							ctx.txn.set(&key.encode(), reifydb_core::row::EncodedRow(
                                reifydb_core::util::CowVec::new(serialized)
                            ))?;

							// Emit this row as new
							// distinct value
							new_distinct_rows
								.push(row_id);

							// Add columns for this
							// row - simplified,
							// just clone the row
							// In production, we'd
							// properly handle
							// column slicing
						} else if let Some(data) =
							existing
						{
							// Update the count for
							// existing distinct
							// value
							let bytes = data
								.row
								.as_ref();
							let mut entry: DistinctEntry = bincode::deserialize(bytes)
                                .map_err(|e| reifydb_core::Error(reifydb_core::internal_error!(
                                    "Failed to deserialize: {}", e
                                )))?;
							entry.count += 1;
							let serialized = bincode::serialize(&entry)
                                .map_err(|e| reifydb_core::Error(reifydb_core::internal_error!(
                                    "Failed to serialize: {}", e
                                )))?;
							ctx.txn.set(&key.encode(), reifydb_core::row::EncodedRow(
                                reifydb_core::util::CowVec::new(serialized)
                            ))?;
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
                            after: after.clone(),
                        });
					}
				}

				FlowDiff::Remove {
					source,
					row_ids,
					before,
				} => {
					let mut removed_distinct_rows =
						Vec::new();

					for (idx, &row_id) in
						row_ids.iter().enumerate()
					{
						let row_hash = Self::hash_row(
							before, idx,
						);
						let key = FlowDistinctStateKey::new(
                            self.flow_id,
                            self.node_id,
                            row_hash,
                        );

						let existing = ctx
							.txn
							.get(&key.encode())?;
						if let Some(data) = existing {
							let bytes = data
								.row
								.as_ref();
							let mut entry: DistinctEntry = bincode::deserialize(bytes)
                                .map_err(|e| reifydb_core::Error(reifydb_core::internal_error!(
                                    "Failed to deserialize: {}", e
                                )))?;

							if entry.count > 1 {
								// Still have
								// other instances
								entry.count -=
									1;
								let serialized = bincode::serialize(&entry)
                                    .map_err(|e| reifydb_core::Error(reifydb_core::internal_error!(
                                        "Failed to serialize: {}", e
                                    )))?;
								ctx.txn.set(&key.encode(), reifydb_core::row::EncodedRow(
                                    reifydb_core::util::CowVec::new(serialized)
                                ))?;
							} else {
								// Last instance
								// - remove from
								// state and emit
								// retraction
								ctx.txn
									.remove(&key
									.encode(
									))?;

								removed_distinct_rows.push(reifydb_core::RowNumber(entry.first_row_id));
							}
						}
					}

					if !removed_distinct_rows.is_empty() {
						output_diffs.push(FlowDiff::Remove {
                            source: *source,
                            row_ids: removed_distinct_rows,
                            before: before.clone(),
                        });
					}
				}

				FlowDiff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					// Handle update as remove + insert
					// First process the remove
					let remove_diff = FlowDiff::Remove {
						source: *source,
						row_ids: row_ids.clone(),
						before: before.clone(),
					};
					let remove_change =
						FlowChange::new(vec![
							remove_diff,
						]);
					let remove_result = self
						.apply(ctx, &remove_change)?;
					output_diffs
						.extend(remove_result.diffs);

					// Then process the insert
					let insert_diff = FlowDiff::Insert {
						source: *source,
						row_ids: row_ids.clone(),
						after: after.clone(),
					};
					let insert_change =
						FlowChange::new(vec![
							insert_diff,
						]);
					let insert_result = self
						.apply(ctx, &insert_change)?;
					output_diffs
						.extend(insert_result.diffs);
				}
			}
		}

		Ok(FlowChange {
			diffs: output_diffs,
			metadata: change.metadata.clone(),
		})
	}
}
