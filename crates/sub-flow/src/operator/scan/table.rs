// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::{flow::FlowNodeId, primitive::PrimitiveId, table::TableDef},
		change::{Change, Diff},
	},
	key::row::RowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{fragment::Fragment, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{Operator, operator::sink::decode_dictionary_columns, transaction::FlowTransaction};

pub struct PrimitiveTableOperator {
	node: FlowNodeId,
	table: TableDef,
}

impl PrimitiveTableOperator {
	pub fn new(node: FlowNodeId, table: TableDef) -> Self {
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

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> reifydb_type::Result<Change> {
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

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		if rows.is_empty() {
			return Ok(Columns::from_table_def(&self.table));
		}

		let schema: Schema = (&self.table.columns).into();
		let fields = schema.fields();

		// Pre-allocate columns with capacity
		let mut columns_vec: Vec<Column> = Vec::with_capacity(fields.len());
		for field in fields.iter() {
			columns_vec.push(Column {
				name: Fragment::internal(&field.name),
				data: ColumnData::with_capacity(field.constraint.get_type(), rows.len()),
			});
		}
		let mut row_numbers = Vec::with_capacity(rows.len());

		for row_num in rows {
			let key = RowKey::encoded(PrimitiveId::table(self.table.id), *row_num);
			if let Some(encoded) = txn.get(&key)? {
				row_numbers.push(*row_num);
				// Decode each column value directly
				for (i, _field) in fields.iter().enumerate() {
					let value = schema.get_value(&encoded, i);
					columns_vec[i].data.push_value(value);
				}
			}
		}

		if row_numbers.is_empty() {
			Ok(Columns::from_table_def(&self.table))
		} else {
			Ok(Columns {
				row_numbers: CowVec::new(row_numbers),
				columns: CowVec::new(columns_vec),
			})
		}
	}
}
