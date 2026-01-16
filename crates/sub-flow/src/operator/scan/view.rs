// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::named::EncodedValuesNamedLayout,
	interface::catalog::{flow::FlowNodeId, primitive::PrimitiveId, view::ViewDef},
	key::row::RowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_sdk::flow::FlowChange;
use reifydb_type::{fragment::Fragment, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{Operator, transaction::FlowTransaction};

pub struct PrimitiveViewOperator {
	node: FlowNodeId,
	view: ViewDef,
}

impl PrimitiveViewOperator {
	pub fn new(node: FlowNodeId, view: ViewDef) -> Self {
		Self {
			node,
			view,
		}
	}
}

impl Operator for PrimitiveViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		Ok(change)
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		if rows.is_empty() {
			return Ok(Columns::from_view_def(&self.view));
		}

		// Get schema from view def
		let layout: EncodedValuesNamedLayout = (&self.view).into();
		let names = layout.names();
		let fields = layout.fields();

		// Pre-allocate columns with capacity
		let mut columns_vec: Vec<Column> = Vec::with_capacity(names.len());
		for (name, field) in names.iter().zip(fields.fields.iter()) {
			columns_vec.push(Column {
				name: Fragment::internal(name),
				data: ColumnData::with_capacity(field.r#type, rows.len()),
			});
		}
		let mut row_numbers = Vec::with_capacity(rows.len());

		// Fetch and decode each row directly into columns
		for row_num in rows {
			let key = RowKey::encoded(PrimitiveId::view(self.view.id), *row_num);
			if let Some(encoded) = txn.get(&key)? {
				row_numbers.push(*row_num);
				// Decode each column value directly
				for (i, _field) in fields.fields.iter().enumerate() {
					let value = layout.get_value_by_idx(&encoded, i);
					columns_vec[i].data.push_value(value);
				}
			}
		}

		if row_numbers.is_empty() {
			Ok(Columns::from_view_def(&self.view))
		} else {
			Ok(Columns {
				row_numbers: CowVec::new(row_numbers),
				columns: CowVec::new(columns_vec),
			})
		}
	}
}
