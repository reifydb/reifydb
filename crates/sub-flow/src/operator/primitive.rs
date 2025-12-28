// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	Row,
	interface::{FlowDef, FlowNodeId, PrimitiveId, TableDef, ViewDef},
	key::{EncodableKey, RowKey},
	value::column::Columns,
};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_flow_operator_sdk::FlowChange;
use reifydb_type::RowNumber;

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
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		let mut found_rows: Vec<Row> = Vec::new();
		for row_num in rows {
			let key = RowKey {
				primitive: PrimitiveId::table(self.table.id),
				row: *row_num,
			}
			.encode();
			if let Some(values) = txn.get(&key).await? {
				found_rows.push(Row {
					number: *row_num,
					encoded: values,
					layout: (&self.table).into(),
				});
			}
		}
		if found_rows.is_empty() {
			Ok(Columns::from_table_def(&self.table))
		} else if found_rows.len() == 1 {
			Ok(Columns::from_row(&found_rows[0]))
		} else {
			// Combine multiple rows into single Columns
			let mut result = Columns::from_row(&found_rows[0]);
			for row in &found_rows[1..] {
				let cols = Columns::from_row(row);
				// Extend row numbers
				result.row_numbers.make_mut().extend(cols.row_numbers.iter().copied());
				// Extend each column
				for (i, col) in cols.columns.into_iter().enumerate() {
					result.columns.make_mut()[i]
						.extend(col)
						.expect("schema mismatch in primitive pull");
				}
			}
			Ok(result)
		}
	}
}

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

#[async_trait]
impl Operator for PrimitiveViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		let mut found_rows: Vec<Row> = Vec::new();
		for row_num in rows {
			let key = RowKey {
				primitive: PrimitiveId::view(self.view.id),
				row: *row_num,
			}
			.encode();
			if let Some(encoded) = txn.get(&key).await? {
				found_rows.push(Row {
					number: *row_num,
					encoded,
					layout: (&self.view).into(),
				});
			}
		}
		if found_rows.is_empty() {
			Ok(Columns::from_view_def(&self.view))
		} else if found_rows.len() == 1 {
			Ok(Columns::from_row(&found_rows[0]))
		} else {
			let mut result = Columns::from_row(&found_rows[0]);
			for row in &found_rows[1..] {
				let cols = Columns::from_row(row);
				result.row_numbers.make_mut().extend(cols.row_numbers.iter().copied());
				for (i, col) in cols.columns.into_iter().enumerate() {
					result.columns.make_mut()[i]
						.extend(col)
						.expect("schema mismatch in primitive pull");
				}
			}
			Ok(result)
		}
	}
}

pub struct PrimitiveFlowOperator {
	node: FlowNodeId,
	flow: FlowDef,
}

impl PrimitiveFlowOperator {
	pub fn new(node: FlowNodeId, flow: FlowDef) -> Self {
		Self {
			node,
			flow,
		}
	}
}

#[async_trait]
impl Operator for PrimitiveFlowOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	async fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> crate::Result<Columns> {
		// TODO: Implement flow pull
		unimplemented!()
	}
}
