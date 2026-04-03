// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowNodeId, shape::ShapeId, table::Table},
		change::{Change, Diff},
	},
	key::row::RowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{
	Result,
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{Operator, operator::sink::decode_dictionary_columns, transaction::FlowTransaction};

pub struct PrimitiveTableOperator {
	node: FlowNodeId,
	table: Table,
}

impl PrimitiveTableOperator {
	pub fn new(node: FlowNodeId, table: Table) -> Self {
		Self {
			node,
			table,
		}
	}
}

impl Operator for PrimitiveTableOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::Insert {
						post: decoded,
					}
				}
				Diff::Update {
					pre,
					post,
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, txn)?;
					decode_dictionary_columns(&mut decoded_post, txn)?;
					Diff::Update {
						pre: decoded_pre,
						post: decoded_post,
					}
				}
				Diff::Remove {
					pre,
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::Remove {
						pre: decoded,
					}
				}
			});
		}
		Ok(Change::from_flow(self.node, change.version, decoded_diffs))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		if rows.is_empty() {
			return Ok(Columns::from_table(&self.table));
		}

		let shape: RowShape = (&self.table.columns).into();
		let fields = shape.fields();

		// Pre-allocate columns with capacity
		let mut columns_vec: Vec<Column> = Vec::with_capacity(fields.len());
		for field in fields.iter() {
			columns_vec.push(Column {
				name: Fragment::internal(&field.name),
				data: ColumnData::with_capacity(field.constraint.get_type(), rows.len()),
			});
		}
		let mut row_numbers = Vec::with_capacity(rows.len());
		let mut created_at = Vec::with_capacity(rows.len());
		let mut updated_at = Vec::with_capacity(rows.len());

		for row_num in rows {
			let key = RowKey::encoded(ShapeId::table(self.table.id), *row_num);
			if let Some(encoded) = txn.get(&key)? {
				row_numbers.push(*row_num);
				created_at.push(DateTime::from_nanos(encoded.created_at_nanos()));
				updated_at.push(DateTime::from_nanos(encoded.updated_at_nanos()));
				// Decode each column value directly
				for (i, _field) in fields.iter().enumerate() {
					let value = shape.get_value(&encoded, i);
					columns_vec[i].data.push_value(value);
				}
			}
		}

		if row_numbers.is_empty() {
			Ok(Columns::from_table(&self.table))
		} else {
			Ok(Columns {
				row_numbers: CowVec::new(row_numbers),
				created_at: CowVec::new(created_at),
				updated_at: CowVec::new(updated_at),
				columns: CowVec::new(columns_vec),
			})
		}
	}
}
