// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, Row,
	interface::{FlowNodeId, MultiVersionQueryTransaction, SourceId, TableDef, ViewDef},
	key::{EncodableKey, RowKey},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::RowNumber;

use crate::{Operator, flow::FlowChange};

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
		_txn: &mut StandardCommandTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	fn get_rows(
		&self,
		txn: &mut StandardCommandTransaction,
		rows: &[RowNumber],
		version: CommitVersion,
	) -> crate::Result<Vec<Option<Row>>> {
		txn.with_multi_query_as_of_inclusive(version, |query_txn| {
			let mut result = Vec::with_capacity(rows.len());
			for row in rows {
				result.push(query_txn
					.get(&RowKey {
						source: SourceId::table(self.table.id),
						row: *row,
					}
					.encode())?
					.map(|mv| Row {
						number: *row,
						encoded: mv.values,
						layout: (&self.table).into(),
					}));
			}
			Ok(result)
		})
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
		_txn: &mut StandardCommandTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	fn get_rows(
		&self,
		txn: &mut StandardCommandTransaction,
		rows: &[RowNumber],
		version: CommitVersion,
	) -> crate::Result<Vec<Option<Row>>> {
		txn.with_multi_query_as_of_inclusive(version, |query_txn| {
			let mut result = Vec::with_capacity(rows.len());
			for row in rows {
				result.push(query_txn
					.get(&RowKey {
						source: SourceId::view(self.view.id),
						row: *row,
					}
					.encode())?
					.map(|mv| Row {
						number: *row,
						encoded: mv.values,
						layout: (&self.view).into(),
					}));
			}
			Ok(result)
		})
	}
}
