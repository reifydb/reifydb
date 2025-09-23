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
use reifydb_type::{Error, Fragment, ROW_NUMBER_COLUMN_NAME, Value, internal_error};

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

		for (idx, field) in ctx.row.layout.fields.iter().enumerate() {
			let value = ctx.row.layout.get_value(&ctx.row.encoded, idx);
			// FIXME maybe some auto conversion needs to happen here
			// Allow undefined values in any field type
			debug_assert!(
				field.value == value.get_type() || value.get_type() == reifydb_type::Type::Undefined,
				"Type mismatch: field expects {:?}, got {:?}",
				field.value,
				value.get_type()
			);

			// Use the field type for the column data, not the value type
			// This ensures undefined values are handled correctly
			let column_type = if value.get_type() == reifydb_type::Type::Undefined {
				field.value
			} else {
				value.get_type()
			};
			let mut data = ColumnData::with_capacity(column_type, 1);
			data.push_value(value);

			let name = ctx.row.layout.get_name(idx).ok_or_else(|| {
				Error(internal_error!("EncodedRowNamedLayout missing name for field at index {}", idx))
			})?;

			columns.push(Column::Computed(ColumnComputed {
				name: Fragment::owned_internal(name),
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
