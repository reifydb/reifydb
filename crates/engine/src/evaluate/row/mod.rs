// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) use reifydb_core::interface::{RowEvaluationContext, RowEvaluator};
use reifydb_core::{
	interface::{ColumnEvaluationContext, ColumnEvaluator, expression::Expression},
	value::{
		column::{Column, ColumnComputed, ColumnData, Columns},
		container::NumberContainer,
	},
};
use reifydb_type::{Fragment, ROW_NUMBER_COLUMN_NAME, Value};

use crate::evaluate::column::StandardColumnEvaluator;

pub struct StandardRowEvaluator {
	evaluator: StandardColumnEvaluator,
}

impl StandardRowEvaluator {
	pub fn new() -> Self {
		Self {
			evaluator: StandardColumnEvaluator::default(),
		}
	}
}

impl Default for StandardRowEvaluator {
	fn default() -> Self {
		Self::new()
	}
}

impl RowEvaluator for StandardRowEvaluator {
	fn evaluate<'a>(&self, ctx: &RowEvaluationContext<'a>, expr: &Expression<'a>) -> crate::Result<Value> {
		let mut columns = Vec::new();

		let row_number_column = Column::Computed(ColumnComputed {
			name: Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
			data: ColumnData::Uint8(NumberContainer::from_vec(vec![ctx.row.number.0])),
		});
		columns.push(row_number_column);

		for (idx, _field) in ctx.row.layout.fields.iter().enumerate() {
			let value = ctx.row.layout.get_value(&ctx.row.encoded, idx);
			let mut data = ColumnData::with_capacity(value.get_type(), 1);
			data.push_value(value);

			columns.push(Column::Computed(ColumnComputed {
				name: Fragment::owned_internal(format!("col_{idx}")), // FIXME no field name
				data,
			}))
		}

		let ctx = ColumnEvaluationContext {
			target: ctx.target.clone(),
			columns: Columns::new(columns),
			row_count: 1,
			take: None,
			params: ctx.params,
		};

		let result = self.evaluator.evaluate(&ctx, &expr)?;

		Ok(result.data().get_value(0))
	}
}
