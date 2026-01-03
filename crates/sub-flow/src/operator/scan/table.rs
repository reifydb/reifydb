// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_core::{
	interface::{FlowNodeId, PrimitiveId, TableDef},
	key::RowKey,
	util::CowVec,
	value::{
		column::{Column, ColumnData, Columns},
		encoded::EncodedValuesNamedLayout,
	},
};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_sdk::FlowChange;
use reifydb_type::{Fragment, RowNumber};

use crate::{Operator, transaction::FlowTransaction};

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

#[async_trait]
impl Operator for PrimitiveTableOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		_txn: &mut FlowTransaction<'_>,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	async fn pull(&self, txn: &mut FlowTransaction<'_>, rows: &[RowNumber]) -> crate::Result<Columns> {
		if rows.is_empty() {
			return Ok(Columns::from_table_def(&self.table));
		}

		// Get schema from table def
		let layout: EncodedValuesNamedLayout = (&self.table).into();
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
			let key = RowKey::encoded(PrimitiveId::table(self.table.id), *row_num);
			if let Some(encoded) = txn.get(&key).await? {
				row_numbers.push(*row_num);
				// Decode each column value directly
				for (i, _field) in fields.fields.iter().enumerate() {
					let value = layout.get_value_by_idx(&encoded, i);
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
