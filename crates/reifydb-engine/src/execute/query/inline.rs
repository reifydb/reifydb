// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{BTreeSet, HashMap},
	sync::Arc,
};

use reifydb_core::{
	ColumnDescriptor, Fragment, Type, Value,
	interface::{
		TableDef, Transaction, evaluate::expression::AliasExpression,
	},
};

use crate::{
	columnar::{
		Column, ColumnData, ColumnQualified, Columns,
		layout::{ColumnLayout, ColumnsLayout},
	},
	evaluate::{EvaluationContext, cast::cast_column_data, evaluate},
	execute::{Batch, ExecutionContext},
};

pub(crate) struct InlineDataNode<T: Transaction> {
	rows: Vec<Vec<AliasExpression>>,
	layout: Option<ColumnsLayout>,
	context: Arc<ExecutionContext>,
	executed: bool,
	_phantom: std::marker::PhantomData<T>,
}

impl<T: Transaction> InlineDataNode<T> {
	pub fn new(
		rows: Vec<Vec<AliasExpression>>,
		context: Arc<ExecutionContext>,
	) -> Self {
		let layout = context.table.as_ref().map(|table| {
			Self::create_columns_layout_from_table(table)
		});

		Self {
			rows,
			layout,
			context,
			executed: false,
			_phantom: std::marker::PhantomData,
		}
	}

	fn create_columns_layout_from_table(table: &TableDef) -> ColumnsLayout {
		let columns = table
			.columns
			.iter()
			.map(|col| ColumnLayout {
				schema: None,
				source: None,
				name: col.name.clone(),
			})
			.collect();

		ColumnsLayout {
			columns,
		}
	}
}

impl<T: Transaction> InlineDataNode<T> {
	pub(crate) fn next(
		&mut self,
		_ctx: &ExecutionContext,
		_rx: &mut crate::StandardTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		if self.executed {
			return Ok(None);
		}

		self.executed = true;

		if self.rows.is_empty() {
			let columns = Columns::empty();
			if self.layout.is_none() {
				self.layout = Some(
					ColumnsLayout::from_columns(&columns),
				);
			}
			return Ok(Some(Batch {
				columns,
			}));
		}

		// Choose execution path based on whether we have table schema
		if self.layout.is_some() {
			self.next_with_table_schema()
		} else {
			self.next_infer_schema()
		}
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone()
	}
}

impl<T: Transaction> InlineDataNode<T> {
	/// Determines the optimal (narrowest) integer type that can hold all
	/// values
	fn find_optimal_integer_type(column: &ColumnData) -> Type {
		let mut min_val = i128::MAX;
		let mut max_val = i128::MIN;
		let mut has_values = false;

		for value in column.iter() {
			match value {
				Value::Int16(v) => {
					has_values = true;
					min_val = min_val.min(v as i128);
					max_val = max_val.max(v as i128);
				}
				Value::Undefined => {
					// Skip undefined values
				}
				_ => {
					// Non-integer value, keep as Int16
					return Type::Int16;
				}
			}
		}

		if !has_values {
			return Type::Int1; // Default to smallest if no values
		}

		// Determine narrowest type that can hold the range
		if min_val >= i8::MIN as i128 && max_val <= i8::MAX as i128 {
			Type::Int1
		} else if min_val >= i16::MIN as i128
			&& max_val <= i16::MAX as i128
		{
			Type::Int2
		} else if min_val >= i32::MIN as i128
			&& max_val <= i32::MAX as i128
		{
			Type::Int4
		} else if min_val >= i64::MIN as i128
			&& max_val <= i64::MAX as i128
		{
			Type::Int8
		} else {
			Type::Int16
		}
	}

	fn next_infer_schema(&mut self) -> crate::Result<Option<Batch>> {
		// Collect all unique column names across all rows
		let mut all_columns: BTreeSet<String> = BTreeSet::new();

		for row in &self.rows {
			for keyed_expr in row {
				let column_name = keyed_expr
					.alias
					.0
					.fragment()
					.to_string();
				all_columns.insert(column_name);
			}
		}

		// Convert each row to a HashMap for easier lookup
		let mut rows_data: Vec<HashMap<String, &AliasExpression>> =
			Vec::new();

		for row in &self.rows {
			let mut row_map: HashMap<String, &AliasExpression> =
				HashMap::new();
			for alias_expr in row {
				let column_name = alias_expr
					.alias
					.0
					.fragment()
					.to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		// Create columns - start with wide types
		let mut columns = Vec::new();

		for column_name in all_columns {
			// First pass: collect all values in a wide column
			let mut all_values = Vec::new();
			let mut first_value_type = Type::Undefined;

			for row_data in &rows_data {
				if let Some(alias_expr) =
					row_data.get(&column_name)
				{
					let ctx = EvaluationContext {
						target_column: None,
						column_policies: Vec::new(),
						columns: Columns::empty(),
						row_count: 1,
						take: None,
						params: &self.context.params,
					};

					let evaluated = evaluate(
						&ctx,
						&alias_expr.expression,
					)?;

					// Take the first value from the
					// evaluated result
					let mut iter = evaluated.data().iter();
					if let Some(value) = iter.next() {
						// Track the first non-undefined
						// value type we see
						if first_value_type
							== Type::Undefined && value
							.get_type()
							!= Type::Undefined
						{
							first_value_type =
								value.get_type(
								);
						}
						all_values.push(value);
					} else {
						all_values
							.push(Value::Undefined);
					}
				} else {
					all_values.push(Value::Undefined);
				}
			}

			// Determine the initial wide type based on what we saw
			let wide_type = if first_value_type.is_integer() {
				Type::Int16 // Start with widest integer type
			} else if first_value_type.is_floating_point() {
				Type::Float8 // Start with widest float type
			} else if first_value_type == Type::Utf8 {
				Type::Utf8
			} else if first_value_type == Type::Bool {
				Type::Bool
			} else {
				Type::Undefined
			};

			// Create the wide column and add all values
			let mut column_data =
				if wide_type == Type::Undefined {
					ColumnData::undefined(all_values.len())
				} else {
					let mut data =
						ColumnData::with_capacity(
							wide_type, 0,
						);

					// Add each value, casting to the wide
					// type if needed
					for value in &all_values {
						if value.get_type()
							== Type::Undefined
						{
							data.push_undefined();
						} else if value.get_type()
							== wide_type
						{
							data.push_value(
								value.clone(),
							);
						} else {
							// Cast to the wide type
							let temp_data = ColumnData::from(value.clone());
							let ctx = EvaluationContext {
							target_column: None,
							column_policies: Vec::new(),
							columns: Columns::empty(),
							row_count: 1,
							take: None,
							params: &self.context.params,
						};

							match cast_column_data(
							&ctx,
							&temp_data,
							wide_type,
							|| Fragment::none(),
						) {
							Ok(casted) => {
								if let Some(casted_value) = casted.iter().next() {
									data.push_value(casted_value);
								} else {
									data.push_undefined();
								}
							}
							Err(_) => {
								data.push_undefined();
							}
						}
						}
					}

					data
				};

			// Now optimize: find the narrowest type and demote if
			// possible
			if wide_type == Type::Int16 {
				let optimal_type =
					Self::find_optimal_integer_type(
						&column_data,
					);
				if optimal_type != Type::Int16 {
					// Demote to the optimal type
					let ctx = EvaluationContext {
						target_column: None,
						column_policies: Vec::new(),
						columns: Columns::empty(),
						row_count: column_data.len(),
						take: None,
						params: &self.context.params,
					};

					if let Ok(demoted) = cast_column_data(
						&ctx,
						&column_data,
						optimal_type,
						|| Fragment::none(),
					) {
						column_data = demoted;
					}
				}
			}
			// Could add similar optimization for Float8 -> Float4
			// if needed

			columns.push(Column::ColumnQualified(
				ColumnQualified {
					name: column_name,
					data: column_data,
				},
			));
		}

		let columns = Columns::new(columns);
		self.layout = Some(ColumnsLayout::from_columns(&columns));

		Ok(Some(Batch {
			columns,
		}))
	}

	fn next_with_table_schema(&mut self) -> crate::Result<Option<Batch>> {
		let table = self.context.table.as_ref().unwrap(); // Safe because layout is Some
		let layout = self.layout.as_ref().unwrap(); // Safe because we're in this path

		// Convert rows to HashMap for easier column lookup
		let mut rows_data: Vec<HashMap<String, &AliasExpression>> =
			Vec::new();

		for row in &self.rows {
			let mut row_map: HashMap<String, &AliasExpression> =
				HashMap::new();
			for alias_expr in row {
				let column_name = alias_expr
					.alias
					.0
					.fragment()
					.to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		// Create columns columns based on table schema
		let mut columns_columns = Vec::new();

		for column_layout in &layout.columns {
			let mut column_data = ColumnData::undefined(0);

			// Find the corresponding table column for policies
			let table_column = table
				.columns
				.iter()
				.find(|col| col.name == column_layout.name)
				.unwrap(); // Safe because layout came from table

			for row_data in &rows_data {
				if let Some(alias_expr) =
					row_data.get(&column_layout.name)
				{
					// Create ColumnDescriptor with table
					// context
					let column_descriptor = ColumnDescriptor::new()
                        .with_table(&table.name)
                        .with_column(&table_column.name)
                        .with_column_type(table_column.ty)
                        .with_policies(
                            table_column.policies.iter().map(|cp| cp.policy.clone()).collect(),
                        );

					let ctx = EvaluationContext {
						target_column: Some(
							column_descriptor,
						),
						column_policies: table_column
							.policies
							.iter()
							.map(|cp| {
								cp.policy
									.clone()
							})
							.collect(),
						columns: Columns::empty(),
						row_count: 1,
						take: None,
						params: &self.context.params,
					};

					let evaluated = evaluate(
						&ctx,
						&alias_expr.expression,
					)?;

					// Ensure we always add exactly one
					// value
					let eval_len = evaluated.data().len();
					if eval_len == 1 {
						column_data.extend(
							evaluated
								.data()
								.clone(),
						)?;
					} else if eval_len == 0 {
						// If evaluation returned empty,
						// push undefined
						column_data.push_value(
							Value::Undefined,
						);
					} else {
						// This shouldn't happen for
						// single-row evaluation
						// but if it does, take only the
						// first value
						let first_value = evaluated.data().iter().next()
							.unwrap_or(Value::Undefined);
						column_data.push_value(
							first_value,
						);
					}
				} else {
					column_data
						.push_value(Value::Undefined);
				}
			}

			columns_columns.push(Column::ColumnQualified(
				ColumnQualified {
					name: column_layout.name.clone(),
					data: column_data,
				},
			));
		}

		let columns = Columns::new(columns_columns);

		Ok(Some(Batch {
			columns,
		}))
	}
}
