// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, ringbuffer::RingBuffer},
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{Result, fragment::Fragment};

use crate::operator::{HostOperator, host::HostContext, sink::decode_dictionary_columns};

pub struct SourceRingBufferOperator {
	operator: OperatorId,
	ringbuffer: RingBuffer,
}

impl SourceRingBufferOperator {
	pub fn new(operator: OperatorId, ringbuffer: RingBuffer) -> Self {
		Self {
			operator,
			ringbuffer,
		}
	}
}

impl HostOperator for SourceRingBufferOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
					..
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, host)?;
					Diff::insert(decoded)
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, host)?;
					decode_dictionary_columns(&mut decoded_post, host)?;
					Diff::update(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
					..
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, host)?;
					Diff::remove(decoded)
				}
			});
		}
		Ok(Change::from_flow(self.operator, change.version, decoded_diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		Some(self.output_schema())
	}
}

impl SourceRingBufferOperator {
	pub fn output_schema(&self) -> Columns {
		let columns: Vec<ColumnWithName> = self
			.ringbuffer
			.columns
			.iter()
			.map(|col| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(col.constraint.get_type(), 0),
			})
			.collect();
		Columns::new(columns)
	}
}
