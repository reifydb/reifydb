// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{flow::FlowNodeId, series::SeriesDef},
		change::Change,
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{
	Result,
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{row_number::RowNumber, r#type::Type},
};

use crate::{Operator, transaction::FlowTransaction};

pub struct PrimitiveSeriesOperator {
	node: FlowNodeId,
	series: SeriesDef,
}

impl PrimitiveSeriesOperator {
	pub fn new(node: FlowNodeId, series: SeriesDef) -> Self {
		Self {
			node,
			series,
		}
	}
}

impl Operator for PrimitiveSeriesOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		Ok(Change::from_flow(self.node, change.version, change.diffs))
	}

	fn pull(&self, _txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		if rows.is_empty() {
			return Ok(self.empty_columns());
		}

		// Series pull returns empty columns since series data is keyed differently
		// than tables. Flow changes are pushed via apply() instead.
		Ok(self.empty_columns())
	}
}

impl PrimitiveSeriesOperator {
	fn empty_columns(&self) -> Columns {
		let mut columns = Vec::with_capacity(1 + self.series.columns.len());

		// Timestamp column
		columns.push(Column {
			name: Fragment::internal("timestamp"),
			data: ColumnData::with_capacity(Type::Int8, 0),
		});

		// Data columns
		for col in &self.series.columns {
			columns.push(Column {
				name: Fragment::internal(&col.name),
				data: ColumnData::with_capacity(col.constraint.get_type(), 0),
			});
		}

		Columns {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}
}
