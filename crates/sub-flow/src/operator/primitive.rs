// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	Row,
	interface::{FlowDef, FlowNodeId, PrimitiveId, TableDef, ViewDef},
	key::{EncodableKey, RowKey},
};
use reifydb_engine::StandardRowEvaluator;
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
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	async fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		let mut result = Vec::with_capacity(rows.len());
		for row in rows {
			let key = RowKey {
				primitive: PrimitiveId::table(self.table.id),
				row: *row,
			}
			.encode();
			result.push(txn.get(&key).await?.map(|values| Row {
				number: *row,
				encoded: values,
				layout: (&self.table).into(),
			}));
		}
		Ok(result)
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
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	async fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		let mut result = Vec::with_capacity(rows.len());
		for row in rows {
			let key = RowKey {
				primitive: PrimitiveId::view(self.view.id),
				row: *row,
			}
			.encode();
			result.push(txn.get(&key).await?.map(|encoded| Row {
				number: *row,
				encoded,
				layout: (&self.view).into(),
			}));
		}
		Ok(result)
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
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		Ok(change)
	}

	async fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		// let mut result = Vec::with_capacity(rows.len());
		// for row in rows {
		// 	result.push(txn
		// 		.get(&RowKey {
		// 			primitive: PrimitiveId::flow(self.flow.id),
		// 			row: *row,
		// 		}
		// 		.encode())?
		// 		.map(|encoded| Row {
		// 			number: *row,
		// 			encoded,
		// 			layout: (&self.flow).into(),
		// 		}));
		// }
		// Ok(result)
		unimplemented!()
	}
}
