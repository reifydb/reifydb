// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{flow::FlowNodeId, series::Series},
		change::Change,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
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
	series: Series,
}

impl PrimitiveSeriesOperator {
	pub fn new(node: FlowNodeId, series: Series) -> Self {
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
		Ok(Change::from_flow(self.node, change.version, change.diffs, change.changed_at))
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
		columns.push(ColumnWithName {
			name: Fragment::internal("timestamp"),
			data: ColumnBuffer::with_capacity(Type::Int8, 0),
		});

		// Data columns
		for col in &self.series.columns {
			columns.push(ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(col.constraint.get_type(), 0),
			});
		}

		Columns {
			row_numbers: CowVec::new(Vec::new()),
			created_at: CowVec::new(Vec::new()),
			updated_at: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}
}
