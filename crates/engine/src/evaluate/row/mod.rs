// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) use reifydb_core::interface::{RowEvaluationContext, RowEvaluator};
use reifydb_core::{
	Row,
	interface::{ColumnDef, ColumnEvaluationContext, ColumnEvaluator, expression::Expression},
	value::{
		column::{Column, ColumnData, Columns},
		container::NumberContainer,
		encoded::EncodedValuesNamedLayout,
	},
};
use reifydb_type::{Error, Fragment, Params, ROW_NUMBER_COLUMN_NAME, Type, Value, internal_error};

use crate::evaluate::column::{StandardColumnEvaluator, cast};

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

		let row_number_column = Column {
			name: Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
			data: ColumnData::Uint8(NumberContainer::from_vec(vec![ctx.row.number.0])),
		};
		columns.push(row_number_column);

		for (idx, field) in ctx.row.layout.fields.iter().enumerate() {
			let value = ctx.row.layout.get_value(&ctx.row.encoded, idx);
			// FIXME maybe some auto conversion needs to happen here
			// Allow undefined values in any field type
			debug_assert!(
				field.r#type == value.get_type() || value.get_type() == reifydb_type::Type::Undefined,
				"Type mismatch: field expects {:?}, got {:?}",
				field.r#type,
				value.get_type()
			);

			// Use the field type for the column data, not the value type
			// This ensures undefined values are handled correctly
			let column_type = if value.get_type() == reifydb_type::Type::Undefined {
				field.r#type
			} else {
				value.get_type()
			};
			let mut data = if column_type == Type::Undefined {
				ColumnData::undefined(0)
			} else {
				ColumnData::with_capacity(column_type, 1)
			};
			data.push_value(value);

			let name = ctx.row.layout.get_name(idx).ok_or_else(|| {
				Error(internal_error!("EncodedRowNamedLayout missing name for field at index {}", idx))
			})?;

			columns.push(Column {
				name: Fragment::owned_internal(name),
				data,
			})
		}

		let ctx = ColumnEvaluationContext {
			target: ctx.target.clone(),
			columns: Columns::new(columns),
			row_count: 1,
			take: None,
			params: ctx.params,
			stack: &reifydb_core::stack::Stack::new(),
		};

		let result = self.evaluator.evaluate(&ctx, &expr)?;

		Ok(result.data().get_value(0))
	}
}

impl StandardRowEvaluator {
	pub fn coerce(&self, row: &Row, target_columns: &[ColumnDef]) -> crate::Result<Row> {
		let mut source_columns = Vec::new();

		for (idx, field) in row.layout.fields.iter().enumerate() {
			let value = row.layout.get_value(&row.encoded, idx);

			let mut data = if field.r#type == Type::Undefined {
				ColumnData::undefined(0)
			} else {
				ColumnData::with_capacity(field.r#type, 1)
			};
			data.push_value(value);

			let name = row.layout.get_name(idx).ok_or_else(|| {
				Error(internal_error!("EncodedRowNamedLayout missing name for field at index {}", idx))
			})?;

			source_columns.push(Column {
				name: Fragment::owned_internal(name),
				data,
			});
		}

		let ctx = ColumnEvaluationContext {
			target: None,
			columns: Columns::new(source_columns),
			row_count: 1,
			take: None,
			params: &Params::None,
			stack: &reifydb_core::stack::Stack::new(),
		};

		let mut values = Vec::with_capacity(target_columns.len());
		let mut names = Vec::with_capacity(target_columns.len());
		let mut types = Vec::with_capacity(target_columns.len());

		for target_col in target_columns.iter() {
			let r#type = target_col.constraint.get_type();

			let value = if let Some(source_column) = ctx.columns.column(&target_col.name) {
				let lazy_frag = Fragment::owned_internal(&target_col.name);
				let coerced = cast::cast_column_data(&ctx, source_column.data(), r#type, &lazy_frag)?;
				coerced.get_value(0)
			} else {
				Value::Undefined
			};
			values.push(value);
			names.push(target_col.name.clone());
			types.push(r#type);
		}

		let layout = EncodedValuesNamedLayout::new(names.into_iter().zip(types.into_iter()));
		let mut encoded = layout.allocate_row();
		layout.set_values(&mut encoded, &values);

		Ok(Row {
			number: row.number,
			encoded,
			layout,
		})
	}
}
