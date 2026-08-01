// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, ringbuffer::RingBuffer},
		change::{Change, Diff},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_value::{Result, fragment::Fragment};

use crate::operator::sink::decode_dictionary_columns;

pub struct SourceRingBufferOperator {
	node: OperatorId,
	ringbuffer: RingBuffer,
}

impl SourceRingBufferOperator {
	pub fn new(node: OperatorId, ringbuffer: RingBuffer) -> Self {
		Self {
			node,
			ringbuffer,
		}
	}
}

impl Operator for SourceRingBufferOperator {
	fn id(&self) -> OperatorId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
					..
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::insert(decoded)
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, txn)?;
					decode_dictionary_columns(&mut decoded_post, txn)?;
					Diff::update(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
					..
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::remove(decoded)
				}
			});
		}
		Ok(Change::from_flow(self.node, change.version, decoded_diffs, change.changed_at))
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
