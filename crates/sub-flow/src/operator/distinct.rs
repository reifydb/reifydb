// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	BitVec, EncodedKey,
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, FlowNodeId, Params, Transaction, expression::Expression},
	util::CowVec,
	value::{column::Columns, row::EncodedRow},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_hash::{Hash128, xxh3_128};
use reifydb_type::{Error, Value, internal_error};
use serde::{Deserialize, Serialize};

use crate::operator::{
	Operator,
	transform::{TransformOperator, stateful::RawStatefulOperator},
};

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
}

impl<T: Transaction> Operator<T> for DistinctOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		// TODO: Implement single-row distinct processing
		// For now, just pass through all changes
		Ok(change)
	}
}

// Commented out old implementation for reference
// impl DistinctOperator {
// Create a BitVec mask for the specified row indices
// fn create_mask_for_indices(total_rows: usize, indices: &[usize]) -> BitVec {
// let mut mask = vec![false; total_rows];
// for &idx in indices {
// if idx < total_rows {
// mask[idx] = true;
// }
// }
// BitVec::from(mask)
// }
//
// Compute hash for a row based on expressions
// fn hash_row_with_expressions(
// evaluator: &StandardEvaluator,
// row_idx: usize,
// columns: &Columns,
// expressions: &[Expression<'static>],
// ) -> crate::Result<Hash128> {
// let eval_ctx = EvaluationContext {
// target: None,
// columns: columns.clone(),
// row_count: columns.row_count(),
// take: Some(&[row_idx]),
// params: &Params::None,
// };
//
// let mut hasher = xxh3_128::Hasher::default();
// for expr in expressions {
// let column = evaluator.evaluate(&eval_ctx, expr)?;
// let value = column.get_value(0);
// Hash the value using its bytes representation
// let value_bytes = value.to_string();
// hasher.update(value_bytes.as_bytes());
// }
//
// Ok(hasher.finish())
// }
//
// Extract values for a specific row based on expressions
// fn extract_row_values(
// evaluator: &StandardEvaluator,
// row_idx: usize,
// columns: &Columns,
// expressions: &[Expression<'static>],
// ) -> crate::Result<Vec<Value>> {
// let eval_ctx = EvaluationContext {
// target: None,
// columns: columns.clone(),
// row_count: columns.row_count(),
// take: Some(&[row_idx]),
// params: &Params::None,
// };
//
// let mut values = Vec::new();
// for expr in expressions {
// let column = evaluator.evaluate(&eval_ctx, expr)?;
// let value = column.get_value(0);
// values.push(value);
// }
//
// Ok(values)
// }
// }
//
// impl<T: Transaction> Operator<T> for DistinctOperator {
// fn apply(
// &self,
// txn: &mut StandardCommandTransaction<T>,
// change: FlowChange,
// evaluator: &StandardEvaluator,
// ) -> crate::Result<FlowChange> {
// let mut output_diffs = Vec::new();
//
// for diff in change.diffs {
// match diff {
// FlowDiff::Insert {
// source,
// rows: row_ids,
// post: after,
// } => {
// Track distinct rows for this batch
// let mut distinct_row_indices = Vec::new();
//
// Process each row individually to check distinctness
// for (row_idx, &row_id) in row_ids.iter().enumerate() {
// Compute hash for this row
// let row_hash = Self::hash_row_with_expressions(
// evaluator,
// row_idx,
// &after,
// &self.expressions,
// )?;
//
// Check if this hash exists in our state
// let hash_key = EncodedKey::new(row_hash.to_be_bytes().to_vec());
//
// match self.state_get(txn, &hash_key)? {
// None => {
// New distinct value - add it to the state
// and output
// distinct_row_indices.push(row_idx);
//
// let entry = DistinctEntry {
// count: 1,
// first_row_id: row_id,
// row_data: Self::extract_row_values(
// evaluator,
// row_idx,
// &after,
// &self.expressions,
// )?,
// };
//
// let serialized = serde_json::to_vec(&entry).map_err(|e| {
// Error(internal_error!("Failed to serialize DistinctEntry: {}", e))
// })?;
//
// self.state_set(
// txn,
// &hash_key,
// EncodedRow(CowVec::new(serialized)),
// )?;
// }
// Some(state_bytes) => {
// Already seen - increment the count
// let mut entry: DistinctEntry =
// serde_json::from_slice(state_bytes.as_ref()).map_err(|e| {
// Error(internal_error!(
// "Failed to deserialize DistinctEntry: {}",
// e
// ))
// })?;
//
// entry.count += 1;
//
// let serialized = serde_json::to_vec(&entry).map_err(|e| {
// Error(internal_error!("Failed to serialize DistinctEntry: {}", e))
// })?;
//
// self.state_set(
// txn,
// &hash_key,
// EncodedRow(CowVec::new(serialized)),
// )?;
// }
// }
// }
//
// Only output the distinct rows
// if !distinct_row_indices.is_empty() {
// Filter the columns to only include distinct rows
// let mask = Self::create_mask_for_indices(
// after.row_count(),
// &distinct_row_indices,
// );
// let mut filtered_columns = after.clone();
// filtered_columns.filter(&mask)?;
//
// Extract row_ids for the distinct rows
// let mut new_distinct_rows = Vec::new();
// for &idx in &distinct_row_indices {
// new_distinct_rows.push(row_ids[idx]);
// }
//
// output_diffs.push(FlowDiff::Insert {
// source,
// rows: CowVec::new(new_distinct_rows),
// post: filtered_columns,
// });
// }
// }
// FlowDiff::Remove {
// source,
// rows: row_ids,
// pre: before,
// ..
// } => {
// Track rows that should be removed (count reaches 0)
// let mut remove_indices = Vec::new();
//
// Process each row individually
// for (row_idx, &row_id) in row_ids.iter().enumerate() {
// Compute hash for this row
// let row_hash = Self::hash_row_with_expressions(
// evaluator,
// row_idx,
// &before,
// &self.expressions,
// )?;
//
// let hash_key = EncodedKey::new(row_hash.to_be_bytes().to_vec());
//
// Look up the entry
// match self.state_get(txn, &hash_key)? {
// Some(state_bytes) => {
// let mut entry: DistinctEntry =
// serde_json::from_slice(state_bytes.as_ref()).map_err(|e| {
// Error(internal_error!(
// "Failed to deserialize DistinctEntry: {}",
// e
// ))
// })?;
//
// if entry.count > 1 {
// Decrement count but don't emit a remove
// entry.count -= 1;
//
// let serialized = serde_json::to_vec(&entry).map_err(|e| {
// Error(internal_error!(
// "Failed to serialize DistinctEntry: {}",
// e
// ))
// })?;
//
// self.state_set(
// txn,
// &hash_key,
// EncodedRow(CowVec::new(serialized)),
// )?;
// } else {
// Count is 1, remove from state and emit
// remove
// self.state_remove(txn, &hash_key)?;
// remove_indices.push(row_idx);
// }
// }
// None => {
// This shouldn't happen in normal operation
// Log warning and continue
// eprintln!(
// "Warning: Attempt to remove non-existent distinct value"
// );
// }
// }
// }
//
// Only output the rows that should be removed
// if !remove_indices.is_empty() {
// Filter the columns to only include removed rows
// let mask =
// Self::create_mask_for_indices(before.row_count(), &remove_indices);
// let mut filtered_columns = before.clone();
// filtered_columns.filter(&mask)?;
//
// Extract row_ids for the removed rows
// let mut removed_rows = Vec::new();
// for &idx in &remove_indices {
// removed_rows.push(row_ids[idx]);
// }
//
// output_diffs.push(FlowDiff::Remove {
// source,
// rows: CowVec::new(removed_rows),
// pre: filtered_columns,
// });
// }
// }
// FlowDiff::Update {
// source,
// rows: row_ids,
// pre: before,
// post: after,
// } => {
// Updates are treated as remove + insert for distinctness
// TODO: Optimize for cases where the distinct keys don't
// change
//
// First process removes
// for (row_idx, &row_id) in row_ids.iter().enumerate() {
// let row_hash = Self::hash_row_with_expressions(
// evaluator,
// row_idx,
// &before,
// &self.expressions,
// )?;
// let hash_key = EncodedKey::new(row_hash.to_be_bytes().to_vec());
//
// if let Some(state_bytes) = self.state_get(txn, &hash_key)? {
// let mut entry: DistinctEntry =
// serde_json::from_slice(state_bytes.as_ref()).map_err(|e| {
// Error(internal_error!(
// "Failed to deserialize DistinctEntry: {}",
// e
// ))
// })?;
//
// if entry.count > 1 {
// entry.count -= 1;
// let serialized = serde_json::to_vec(&entry).map_err(|e| {
// Error(internal_error!("Failed to serialize DistinctEntry: {}", e))
// })?;
// self.state_set(
// txn,
// &hash_key,
// EncodedRow(CowVec::new(serialized)),
// )?;
// } else {
// self.state_remove(txn, &hash_key)?;
// }
// }
// }
//
// Then process inserts
// let mut update_indices = Vec::new();
// for (row_idx, &row_id) in row_ids.iter().enumerate() {
// let row_hash = Self::hash_row_with_expressions(
// evaluator,
// row_idx,
// &after,
// &self.expressions,
// )?;
// let hash_key = EncodedKey::new(row_hash.to_be_bytes().to_vec());
//
// match self.state_get(txn, &hash_key)? {
// None => {
// New distinct value for the update
// update_indices.push(row_idx);
//
// let entry = DistinctEntry {
// count: 1,
// first_row_id: row_id,
// row_data: Self::extract_row_values(
// evaluator,
// row_idx,
// &after,
// &self.expressions,
// )?,
// };
//
// let serialized = serde_json::to_vec(&entry).map_err(|e| {
// Error(internal_error!("Failed to serialize DistinctEntry: {}", e))
// })?;
// self.state_set(
// txn,
// &hash_key,
// EncodedRow(CowVec::new(serialized)),
// )?;
// }
// Some(state_bytes) => {
// let mut entry: DistinctEntry =
// serde_json::from_slice(state_bytes.as_ref()).map_err(|e| {
// Error(internal_error!(
// "Failed to deserialize DistinctEntry: {}",
// e
// ))
// })?;
//
// entry.count += 1;
// let serialized = serde_json::to_vec(&entry).map_err(|e| {
// Error(internal_error!("Failed to serialize DistinctEntry: {}", e))
// })?;
// self.state_set(
// txn,
// &hash_key,
// EncodedRow(CowVec::new(serialized)),
// )?;
//
// If this was the first occurrence after the
// update, include it
// if entry.count == 1 {
// update_indices.push(row_idx);
// }
// }
// }
// }
//
// Output update for distinct rows
// if !update_indices.is_empty() {
// let mask = Self::create_mask_for_indices(after.row_count(), &update_indices);
//
// let mut filtered_before = before.clone();
// filtered_before.filter(&mask)?;
//
// let mut filtered_after = after.clone();
// filtered_after.filter(&mask)?;
//
// let mut updated_rows = Vec::new();
// for &idx in &update_indices {
// updated_rows.push(row_ids[idx]);
// }
//
// output_diffs.push(FlowDiff::Update {
// source,
// rows: CowVec::new(updated_rows),
// pre: filtered_before,
// post: filtered_after,
// });
// }
// }
// }
// }
//
// Ok(FlowChange::new(output_diffs))
// }
// }
//
// impl<T: Transaction> TransformOperator<T> for DistinctOperator {
// fn node(&self) -> FlowNodeId {
// self.node
// }
// }
