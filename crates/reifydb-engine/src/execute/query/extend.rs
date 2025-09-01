// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	interface::{
		ColumnDescriptor, Transaction, evaluate::expression::Expression,
	},
	value::columnar::{Columns, layout::ColumnsLayout},
};

use crate::{
	evaluate::{EvaluationContext, evaluate},
	execute::{
		Batch, ExecutionContext, ExecutionPlan, QueryNode,
		query::layout::derive_columns_column_layout,
	},
};

pub(crate) struct ExtendNode<'a, T: Transaction> {
	input: Box<ExecutionPlan<'a, T>>,
	expressions: Vec<Expression<'a>>,
	layout: Option<ColumnsLayout>,
	context: Option<Arc<ExecutionContext>>,
}

impl<'a, T: Transaction> ExtendNode<'a, T> {
	pub fn new(
		input: Box<ExecutionPlan<'a, T>>,
		expressions: Vec<Expression<'a>>,
	) -> Self {
		Self {
			input,
			expressions,
			layout: None,
			context: None,
		}
	}

	fn create_evaluation_context(
		&self,
		expr: &Expression,
		columns: Columns,
		row_count: usize,
	) -> EvaluationContext {
		let mut result = EvaluationContext {
			target_column: None,
			column_policies: Vec::new(),
			columns,
			row_count,
			take: None,
			params: &self.context.as_ref().unwrap().params,
		};

		// Check if this is an alias expression and we have table
		// information
		if let (Expression::Alias(alias_expr), Some(table)) =
			(expr, &self.context.as_ref().unwrap().table)
		{
			let alias_name = alias_expr.alias.name();

			// Find the matching column in the table schema
			if let Some(table_column) = table
				.columns
				.iter()
				.find(|col| col.name == alias_name)
			{
				// Extract ColumnPolicyKind from ColumnPolicy
				let policy_kinds: Vec<_> = table_column
					.policies
					.iter()
					.map(|policy| policy.policy.clone())
					.collect();

				let target_column = ColumnDescriptor::new()
					.with_table(&table.name)
					.with_column(&table_column.name)
					.with_column_type(table_column.ty)
					.with_policies(policy_kinds.clone());

				result.target_column = Some(target_column);
				result.column_policies = policy_kinds;
			}
		}

		result
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for ExtendNode<'a, T> {
	fn initialize(
		&mut self,
		rx: &mut crate::StandardTransaction<'a, T>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(
			self.context.is_some(),
			"ExtendNode::next() called before initialize()"
		);
		let ctx = self.context.as_ref().unwrap();

		while let Some(Batch {
			columns,
		}) = self.input.next(rx)?
		{
			// Start with all existing columns (EXTEND preserves
			// everything)
			let row_count = columns.row_count();
			let mut new_columns =
				columns.into_iter().collect::<Vec<_>>();

			// Add the new derived columns
			for expr in &self.expressions {
				let column = evaluate(
					&self.create_evaluation_context(
						expr,
						Columns::new(
							new_columns.clone(),
						),
						row_count,
					),
					expr,
				)?;

				new_columns.push(column);
			}

			// Create layout combining existing and new columns only
			// once For extend, we preserve all input columns
			// plus the new expressions
			if self.layout.is_none() {
				let layout = if let Some(input_layout) =
					self.input.layout()
				{
					// Combine input layout with new
					// expression layout
					let new_expressions_layout = derive_columns_column_layout(
						&self.expressions,
						ctx.preserve_row_numbers,
					);
					input_layout.extend(
						&new_expressions_layout,
					)?
				} else {
					derive_columns_column_layout(
						&self.expressions,
						ctx.preserve_row_numbers,
					)
				};

				self.layout = Some(layout);
			}

			return Ok(Some(Batch {
				columns: Columns::new(new_columns),
			}));
		}
		Ok(None)
	}

	fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone().or(self.input.layout())
	}
}

pub(crate) struct ExtendWithoutInputNode<'a, T: Transaction> {
	expressions: Vec<Expression<'a>>,
	layout: Option<ColumnsLayout>,
	context: Option<Arc<ExecutionContext>>,
	_phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Transaction> ExtendWithoutInputNode<'a, T> {
	pub fn new(expressions: Vec<Expression<'a>>) -> Self {
		Self {
			expressions,
			layout: None,
			context: None,
			_phantom: std::marker::PhantomData,
		}
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for ExtendWithoutInputNode<'a, T> {
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a, T>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	fn next(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(
			self.context.is_some(),
			"ExtendWithoutInputNode::next() called before initialize()"
		);
		let ctx = self.context.as_ref().unwrap();

		if self.layout.is_some() {
			return Ok(None);
		}

		// Without input, this behaves like MAP without input
		// (generates a single row with the computed expressions)
		let columns = Columns::empty();
		let mut new_columns =
			Vec::with_capacity(self.expressions.len());

		for expr in &self.expressions {
			let evaluation_context = EvaluationContext {
				target_column: None,
				column_policies: Vec::new(),
				columns: columns.clone(),
				row_count: 1, // Generate single row
				take: None,
				params: &ctx.params,
			};

			let column = evaluate(&evaluation_context, expr)?;
			new_columns.push(column);
		}

		let layout = derive_columns_column_layout(
			&self.expressions,
			ctx.preserve_row_numbers,
		);

		self.layout = Some(layout);

		Ok(Some(Batch {
			columns: Columns::new(new_columns),
		}))
	}

	fn layout(&self) -> Option<ColumnsLayout> {
		self.layout.clone()
	}
}
