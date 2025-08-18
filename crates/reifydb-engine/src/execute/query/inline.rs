// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	ColumnDescriptor, Value,
	interface::{
		TableDef, VersionedQueryTransaction,
		evaluate::expression::AliasExpression,
	},
};

use crate::{
	columnar::{
		Column, ColumnData, ColumnQualified, Columns,
		layout::{ColumnLayout, ColumnsLayout},
	},
	evaluate::{EvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan},
};

pub(crate) struct InlineDataNode {
	rows: Vec<Vec<AliasExpression>>,
	layout: Option<ColumnsLayout>,
	context: Arc<ExecutionContext>,
	executed: bool,
}

impl InlineDataNode {
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

impl ExecutionPlan for InlineDataNode {
	fn next(
		&mut self,
		_ctx: &ExecutionContext,
		_rx: &mut dyn VersionedQueryTransaction,
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

	fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone()
	}
}

impl InlineDataNode {
	fn next_infer_schema(&mut self) -> crate::Result<Option<Batch>> {
		// Collect all unique column names across all rows
		let mut all_columns: std::collections::BTreeSet<String> =
			std::collections::BTreeSet::new();

		for row in &self.rows {
			for keyed_expr in row {
				let column_name =
					keyed_expr.alias.0.fragment().to_string();
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
				let column_name =
					alias_expr.alias.0.fragment().to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		// Create columns columns with equal length
		let mut columns_columns = Vec::new();

		for column_name in all_columns {
			let mut column_data = ColumnData::undefined(0);

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
						column_data.push_value(value);
					} else {
						column_data.push_value(
							Value::Undefined,
						);
					}
				} else {
					// Missing column for this row, use
					// Undefined
					column_data
						.push_value(Value::Undefined);
				}
			}

			columns_columns.push(Column::ColumnQualified(
				ColumnQualified {
					name: column_name,
					data: column_data,
				},
			));
		}

		let columns = Columns::new(columns_columns);
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
				let column_name =
					alias_expr.alias.0.fragment().to_string();
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

					column_data.extend(evaluate(
						&ctx,
						&alias_expr.expression,
					)?
					.data()
					.clone())?;
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
