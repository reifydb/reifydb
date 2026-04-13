// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, sync::Arc};

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{Column, columns::Columns, headers::ColumnHeaders},
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_rql::expression::{Expression, name::column_name_from_expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, util::cowvec::CowVec};
use tracing::instrument;

use super::NoopNode;
use crate::{
	Result,
	expression::{
		cast::cast_column_data,
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::volcano::{
		query::{QueryContext, QueryNode},
		udf::{UdfEvalNode, evaluate_udfs_no_input, strip_udf_columns},
	},
};

pub(crate) struct MapNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	udf_names: Vec<String>,
	headers: Option<ColumnHeaders>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl MapNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>) -> Self {
		Self {
			input,
			expressions,
			udf_names: Vec::new(),
			headers: None,
			context: None,
		}
	}
}

impl QueryNode for MapNode {
	#[instrument(name = "volcano::map::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let (input, expressions, udf_names) = UdfEvalNode::wrap_if_needed(
			mem::replace(&mut self.input, Box::new(NoopNode)),
			&self.expressions,
			&ctx.symbols,
		);
		self.input = input;
		self.expressions = expressions;
		self.udf_names = udf_names;

		let compile_ctx = CompileContext {
			functions: &ctx.services.functions,
			symbols: &ctx.symbols,
		};
		let compiled = self
			.expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("compile"))
			.collect();
		self.context = Some((Arc::new(ctx.clone()), compiled));
		let column_names = self.expressions.iter().map(column_name_from_expression).collect();
		self.headers = Some(ColumnHeaders {
			columns: column_names,
		});
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "volcano::map::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "MapNode::next() called before initialize()");

		if let Some(columns) = self.input.next(rx, ctx)? {
			let stored_ctx = &self.context.as_ref().unwrap().0;
			let transform_ctx = TransformContext {
				functions: &stored_ctx.services.functions,
				runtime_context: &stored_ctx.services.runtime_context,
				params: &stored_ctx.params,
			};
			let mut result = self.apply(&transform_ctx, columns)?;
			strip_udf_columns(&mut result, &self.udf_names);

			Ok(Some(result))
		} else {
			Ok(None)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

impl Transform for MapNode {
	fn apply(&self, ctx: &TransformContext, input: Columns) -> Result<Columns> {
		let (stored_ctx, compiled) =
			self.context.as_ref().expect("MapNode::apply() called before initialize()");

		let row_count = input.row_count();
		let session = EvalContext::from_transform(ctx, stored_ctx);
		let mut new_columns = Vec::with_capacity(compiled.len());

		for (expr, compiled_expr) in self.expressions.iter().zip(compiled.iter()) {
			let mut exec_ctx = session.with_eval(input.clone(), row_count);

			if let (Expression::Alias(alias_expr), Some(source)) = (expr, &stored_ctx.source) {
				let alias_name = alias_expr.alias.name();
				if let Some(table_column) = source.columns().iter().find(|col| col.name == alias_name) {
					let column_ident = Fragment::internal(&table_column.name);
					let resolved_column =
						ResolvedColumn::new(column_ident, source.clone(), table_column.clone());
					exec_ctx.target = Some(TargetColumn::Resolved(resolved_column));
				}
			}

			let mut column = compiled_expr.execute(&exec_ctx)?;

			if let Some(target_type) = exec_ctx.target.as_ref().map(|t| t.column_type())
				&& column.data.get_type() != target_type
			{
				let data =
					cast_column_data(&exec_ctx, &column.data, target_type, &expr.lazy_fragment())?;
				column = Column {
					name: column.name,
					data,
				};
			}

			new_columns.push(column);
		}

		Ok(Columns {
			row_numbers: input.row_numbers,
			created_at: input.created_at,
			updated_at: input.updated_at,
			columns: CowVec::new(new_columns),
		})
	}
}

pub(crate) struct MapWithoutInputNode {
	expressions: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	/// When UDFs are present, stores the rewritten expressions and pre-computed UDF result columns.
	udf_columns: Option<Columns>,
	context: Option<(Arc<QueryContext>, Vec<CompiledExpr>)>,
}

impl MapWithoutInputNode {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
			headers: None,
			udf_columns: None,
			context: None,
		}
	}
}

impl QueryNode for MapWithoutInputNode {
	#[instrument(name = "volcano::map::noinput::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		// Extract and evaluate UDFs if present
		if let Some((rewritten, udf_cols)) = evaluate_udfs_no_input(&self.expressions, ctx, rx)? {
			self.expressions = rewritten;
			self.udf_columns = Some(udf_cols);
		}

		let compile_ctx = CompileContext {
			functions: &ctx.services.functions,
			symbols: &ctx.symbols,
		};
		let compiled = self
			.expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("compile"))
			.collect();
		self.context = Some((Arc::new(ctx.clone()), compiled));
		Ok(())
	}

	#[instrument(name = "volcano::map::noinput::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "MapWithoutInputNode::next() called before initialize()");
		let (stored_ctx, compiled) = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let session = EvalContext::from_query(stored_ctx);
		let mut columns = vec![];

		for compiled_expr in compiled {
			// If we have UDF result columns, include them so __udf_N column refs resolve
			let exec_ctx = match &self.udf_columns {
				Some(udf_cols) => session.with_eval(udf_cols.clone(), 1),
				None => session.with_eval_empty(),
			};

			let column = compiled_expr.execute(&exec_ctx)?;

			columns.push(column);
		}

		let columns = Columns::new(columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}
