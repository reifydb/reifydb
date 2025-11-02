// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Row,
	interface::{FlowNodeId, SourceId, TableDef, ViewDef},
	key::{EncodableKey, RowKey},
};
use reifydb_engine::StandardRowEvaluator;
use reifydb_type::RowNumber;

use crate::{Operator, flow::FlowChange, transaction::FlowTransaction};

pub struct SourceTableOperator {
	node: FlowNodeId,
	table: TableDef,
}

impl SourceTableOperator {
	pub fn new(node: FlowNodeId, table: TableDef) -> Self {
		Self {
			node,
			table,
		}
	}
}

impl Operator for SourceTableOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		let mut result = Vec::with_capacity(rows.len());
		for row in rows {
			result.push(txn
				.get(&RowKey {
					source: SourceId::table(self.table.id),
					row: *row,
				}
				.encode())?
				.map(|values| Row {
					number: *row,
					encoded: values,
					layout: (&self.table).into(),
				}));
		}
		Ok(result)
	}
}

pub struct SourceViewOperator {
	node: FlowNodeId,
	view: ViewDef,
}

impl SourceViewOperator {
	pub fn new(node: FlowNodeId, view: ViewDef) -> Self {
		Self {
			node,
			view,
		}
	}
}

impl Operator for SourceViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		let mut result = Vec::with_capacity(rows.len());
		for row in rows {
			result.push(txn
				.get(&RowKey {
					source: SourceId::view(self.view.id),
					row: *row,
				}
				.encode())?
				.map(|encoded| Row {
					number: *row,
					encoded,
					layout: (&self.view).into(),
				}));
		}
		Ok(result)
	}
}
