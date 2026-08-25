// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_value::{Result, error::Error, reifydb_assertions, value::row_number::RowNumber};
use tracing::instrument;

use crate::{
	error::FlowGraphError,
	operator::{HostOperator, append::lane::AppendLanes, host::HostContext},
	timer::Timer,
};

pub mod lane;

#[cfg(test)]
mod tests;

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

pub struct AppendOperator {
	operator: OperatorId,

	parent_schema: Option<Columns>,

	input_nodes: Vec<OperatorId>,

	lanes: AppendLanes,
}

impl AppendOperator {
	pub fn new(
		operator: OperatorId,
		parent_schema: Option<Columns>,
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

	#[cfg(test)]
	pub(crate) fn new_for_state_tests(operator: OperatorId, lanes: AppendLanes) -> Self {
		Self {
			operator,
			parent_schema: None,
			input_nodes: Vec::new(),
			lanes,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent_schema.clone()
	}

	fn parent_index_for_origin(&self, origin: &ChangeOrigin) -> Option<usize> {
		match origin {
			ChangeOrigin::Flow(from_node) => self.input_nodes.iter().position(|n| n == from_node),
			ChangeOrigin::Object(_) => None,
		}
	}

	fn output_row_numbers(&self, parent_index: usize, source: &Columns) -> Vec<RowNumber> {
		source.row_numbers().iter().map(|source_row| self.lanes.stamp(parent_index, *source_row)).collect()
	}
}

impl HostOperator for AppendOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
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
					if let Some(d) = self.translate_append_insert(parent_index, post) {
						result_diffs.push(d);
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					if let Some(d) = self.translate_append_update(parent_index, pre, post) {
						result_diffs.push(d);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					if let Some(d) = self.translate_append_remove(parent_index, pre) {
						result_diffs.push(d);
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result_diffs, change.changed_at))
	}

	fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

impl AppendOperator {
	#[inline]
	#[instrument(name = "flow::operator::append::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn translate_append_insert(&mut self, parent_index: usize, post: Columns) -> Option<Diff> {
		if post.row_count() == 0 {
			return None;
		}
		let output_row_numbers = self.output_row_numbers(parent_index, &post);
		Some(Diff::insert(post.with_row_numbers(output_row_numbers)))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn translate_append_update(&mut self, parent_index: usize, pre: Columns, post: Columns) -> Option<Diff> {
		if post.row_count() == 0 {
			return None;
		}
		let output_row_numbers = self.output_row_numbers(parent_index, &pre);
		let pre_output = pre.with_row_numbers(output_row_numbers.clone());
		let post_output = post.with_row_numbers(output_row_numbers);
		Some(Diff::update(pre_output, post_output))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn translate_append_remove(&mut self, parent_index: usize, pre: Columns) -> Option<Diff> {
		if pre.row_count() == 0 {
			return None;
		}
		let output_row_numbers = self.output_row_numbers(parent_index, &pre);
		Some(Diff::remove(pre.with_row_numbers(output_row_numbers)))
	}
}
