// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use reifydb_core::{
	flow::dag::FlowDag,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{
		dictionary::DictionaryId,
		system_columns::{require_row_numbers, restamp_row_numbers},
		value_type::{
			ValueType,
			field::{FieldType, from_field},
		},
	},
};
use tracing::instrument;

use crate::{
	error::FlowGraphError,
	operator::{
		FORWARDED_SYSTEM_COLUMNS,
		append::lane::{AppendLanes, assign_lanes},
	},
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

fn check_lanes(first: &SchemaRef, second: &SchemaRef) -> Result<()> {
	for field in first.fields() {
		let Ok(other) = second.field_with_name(field.name()) else {
			continue;
		};
		if let (Some(expected), Some(found)) = (lane_type(&from_field(field)?), lane_type(&from_field(other)?))
			&& expected != found
		{
			return Err(Error::from(FlowGraphError::AppendLaneMismatch {
				column: field.name().clone(),
				expected: describe(&expected),
				found: describe(&found),
			}));
		}
	}
	Ok(())
}

fn lane_type(field: &FieldType) -> Option<(ValueType, Option<DictionaryId>)> {
	match field.value_type.as_ref().map(ValueType::inner_type) {
		None | Some(ValueType::Any) => None,
		Some(value_type) => Some((value_type.clone(), field.dictionary_id)),
	}
}

fn describe((value_type, dictionary): &(ValueType, Option<DictionaryId>)) -> String {
	match dictionary {
		Some(dictionary) => format!("{value_type} of dictionary {}", dictionary.0),
		None => value_type.to_string(),
	}
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

	pub fn from_flow(
		flow: &FlowDag,
		operator: OperatorId,
		inputs: &[OperatorId],
		mut parent_schemas: Vec<Option<SchemaRef>>,
	) -> Result<Self> {
		if inputs.len() != 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				operator: "Append",
				expected: "exactly 2",
				found: inputs.len(),
			}));
		}
		if let [Some(first), Some(second)] = parent_schemas.as_slice() {
			check_lanes(first, second)?;
		}
		let parent_schema = parent_schemas.swap_remove(0);
		let lanes = assign_lanes(flow, operator)?;
		Ok(Self::new(operator, parent_schema, inputs.to_vec(), lanes))
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
	restamp_row_numbers(&batch, &FORWARDED_SYSTEM_COLUMNS, row_numbers)
}
