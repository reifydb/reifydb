// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use reifydb_core::interface::{
	catalog::flow::OperatorId,
	change::{Change, ChangeOrigin, Diff},
};
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::system_columns::{SystemColumn, require_row_numbers, with_system_column},
};
use tracing::instrument;

use crate::{
	error::FlowGraphError,
	operator::{append::lane::AppendLanes, forward_system_columns},
};

pub mod lane;

#[cfg(test)]
mod tests;

pub struct AppendOperator {
	operator: OperatorId,

	parent_schema: Option<SchemaRef>,

	input_nodes: Vec<OperatorId>,

	lanes: AppendLanes,
}

impl AppendOperator {
	pub fn new(
		operator: OperatorId,
		parent_schema: Option<SchemaRef>,
		input_nodes: Vec<OperatorId>,
		lanes: AppendLanes,
	) -> Self {
		reifydb_assertions! {
			assert!(
				input_nodes.len() == 2,
				"append is binary: the lane assignment gives each chain leaf exactly one lane, and a \
				 wider node would leave its extra inputs unstamped"
			);
		}

		Self {
			operator,
			parent_schema,
			input_nodes,
			lanes,
		}
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		self.parent_schema.clone()
	}

	fn parent_index_for_origin(&self, origin: &ChangeOrigin) -> Option<usize> {
		match origin {
			ChangeOrigin::Flow(from_node) => self.input_nodes.iter().position(|n| n == from_node),
			ChangeOrigin::Object(_) => None,
		}
	}

	fn output_row_numbers(&self, parent_index: usize, source: &RecordBatch) -> Result<ArrayRef> {
		Ok(Arc::new(UInt64Array::from_iter_values(
			require_row_numbers(source)?
				.iter()
				.map(|source_row| self.lanes.stamp(parent_index, *source_row).0),
		)))
	}
}

impl AppendOperator {
	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn apply(&mut self, change: Change) -> Result<Change> {
		let parent_origin = change.origin.clone();
		let mut result_diffs = Vec::with_capacity(change.diffs.len());

		for diff in change.diffs {
			let diff_origin = diff.origin().cloned().unwrap_or_else(|| parent_origin.clone());
			let parent_index = self.parent_index_for_origin(&diff_origin).ok_or_else(|| {
				Error::from(FlowGraphError::UnknownDiffOrigin {
					operator: "Append",
					origin: Some(format!("{:?}", diff_origin)),
				})
			})?;
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					if let Some(d) = self.translate_append_insert(parent_index, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					if let Some(d) = self.translate_append_update(parent_index, pre, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					if let Some(d) = self.translate_append_remove(parent_index, pre)? {
						result_diffs.push(d);
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result_diffs, change.changed_at))
	}
}

impl AppendOperator {
	#[inline]
	#[instrument(name = "flow::operator::append::insert", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn translate_append_insert(&mut self, parent_index: usize, post: RecordBatch) -> Result<Option<Diff>> {
		if post.num_rows() == 0 {
			return Ok(None);
		}
		let output_row_numbers = self.output_row_numbers(parent_index, &post)?;
		Ok(Some(Diff::insert(restamped(post, output_row_numbers)?)))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::update", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn translate_append_update(
		&mut self,
		parent_index: usize,
		pre: RecordBatch,
		post: RecordBatch,
	) -> Result<Option<Diff>> {
		if post.num_rows() == 0 {
			return Ok(None);
		}
		let output_row_numbers = self.output_row_numbers(parent_index, &pre)?;
		let pre_output = restamped(pre, output_row_numbers.clone())?;
		let post_output = restamped(post, output_row_numbers)?;
		Ok(Some(Diff::update(pre_output, post_output)))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::remove", level = "trace", skip_all, fields(rows = pre.num_rows()))]
	fn translate_append_remove(&mut self, parent_index: usize, pre: RecordBatch) -> Result<Option<Diff>> {
		if pre.num_rows() == 0 {
			return Ok(None);
		}
		let output_row_numbers = self.output_row_numbers(parent_index, &pre)?;
		Ok(Some(Diff::remove(restamped(pre, output_row_numbers)?)))
	}
}

fn restamped(batch: RecordBatch, row_numbers: ArrayRef) -> Result<RecordBatch> {
	forward_system_columns(&with_system_column(batch, SystemColumn::RowNumbers, row_numbers)?)
}
