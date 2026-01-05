// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Query operations compilation.

use bumpalo::collections::Vec as BumpVec;
use reifydb_type::Type;

use super::core::{PlanError, PlanErrorKind, Planner, Result};
use crate::{
	ast::{
		Expr,
		expr::{
			AggregateExpr, DistinctExpr, ExtendExpr, FilterExpr, FromExpr, JoinExpr, Literal, MapExpr,
			MergeExpr, SortExpr, TakeExpr, WindowExpr,
		},
	},
	plan::{
		CatalogColumn, ColumnId, OutputSchema, Plan,
		node::{expr::PlanExpr, query::*},
	},
};

impl<'bump, 'cat> Planner<'bump, 'cat> {
	pub(super) fn compile_from(&mut self, from: &FromExpr<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::expr::FromExpr;

		match from {
			FromExpr::Source(source) => {
				let primitive = self.resolve_primitive(source.namespace, source.name, source.span)?;
				Ok(Plan::Scan(ScanNode {
					primitive,
					alias: source.alias.map(|s| self.bump.alloc_str(s) as &'bump str),
					span: source.span,
				}))
			}
			FromExpr::Variable(var) => {
				let variable = self.resolve_variable(var.variable.name, var.span)?;
				Ok(Plan::VariableSource(VariableSourceNode {
					variable,
					span: var.span,
				}))
			}
			FromExpr::Inline(inline) => {
				// Compile inline data rows
				let mut rows = BumpVec::with_capacity_in(inline.rows.len(), self.bump);
				for row_expr in inline.rows {
					let row = self.compile_inline_row(row_expr)?;
					rows.push(row);
				}
				Ok(Plan::InlineData(InlineDataNode {
					rows: rows.into_bump_slice(),
					span: inline.span,
				}))
			}
			FromExpr::Generator(generator) => {
				let arguments = self.compile_expr_slice(generator.params, None)?;
				Ok(Plan::Generator(GeneratorNode {
					name: self.bump.alloc_str(generator.name),
					arguments,
					span: generator.span,
				}))
			}
			FromExpr::Environment(env) => Ok(Plan::Environment(EnvironmentNode {
				span: env.span,
			})),
		}
	}

	/// Compile an inline row expression.
	fn compile_inline_row(&self, expr: &Expr<'bump>) -> Result<&'bump [&'bump PlanExpr<'bump>]> {
		match expr {
			Expr::Inline(inline) => {
				let mut values = BumpVec::with_capacity_in(inline.fields.len(), self.bump);
				for field in inline.fields.iter() {
					values.push(self.compile_expr(field.value, None)?);
				}
				Ok(values.into_bump_slice())
			}
			Expr::List(list) => self.compile_expr_slice(list.elements, None),
			Expr::Tuple(tuple) => self.compile_expr_slice(tuple.elements, None),
			_ => {
				// Single expression as single-column row
				let compiled = self.compile_expr(expr, None)?;
				Ok(self.bump.alloc_slice_copy(&[compiled]))
			}
		}
	}

	pub(super) fn compile_filter(
		&mut self,
		filter: &FilterExpr<'bump>,
		input: &'bump Plan<'bump>,
	) -> Result<Plan<'bump>> {
		// Use async version to support subqueries in filter predicates
		let predicate = self.compile_expr_with_subqueries(filter.predicate, None)?;
		Ok(Plan::Filter(FilterNode {
			input,
			predicate,
			span: filter.span,
		}))
	}

	pub(super) fn compile_map(
		&mut self,
		map: &MapExpr<'bump>,
		input: Option<&'bump Plan<'bump>>,
	) -> Result<Plan<'bump>> {
		let mut projections = BumpVec::with_capacity_in(map.projections.len(), self.bump);
		for proj_expr in map.projections {
			projections.push(self.compile_projection(proj_expr, None)?);
		}
		Ok(Plan::Project(ProjectNode {
			input,
			projections: projections.into_bump_slice(),
			span: map.span,
		}))
	}

	pub(super) fn compile_extend(
		&mut self,
		extend: &ExtendExpr<'bump>,
		input: Option<&'bump Plan<'bump>>,
	) -> Result<Plan<'bump>> {
		let mut extensions = BumpVec::with_capacity_in(extend.extensions.len(), self.bump);
		for ext_expr in extend.extensions {
			extensions.push(self.compile_projection(ext_expr, None)?);
		}
		Ok(Plan::Extend(ExtendNode {
			input,
			extensions: extensions.into_bump_slice(),
			span: extend.span,
		}))
	}

	pub(super) fn compile_aggregate(
		&mut self,
		agg: &AggregateExpr<'bump>,
		input: &'bump Plan<'bump>,
	) -> Result<Plan<'bump>> {
		let group_by = self.compile_expr_slice(agg.group_by, None)?;
		let mut aggregations = BumpVec::with_capacity_in(agg.aggregations.len(), self.bump);
		for agg_expr in agg.aggregations {
			aggregations.push(self.compile_projection(agg_expr, None)?);
		}
		Ok(Plan::Aggregate(AggregateNode {
			input,
			group_by,
			aggregations: aggregations.into_bump_slice(),
			span: agg.span,
		}))
	}

	pub(super) fn compile_sort(
		&mut self,
		sort: &SortExpr<'bump>,
		input: &'bump Plan<'bump>,
	) -> Result<Plan<'bump>> {
		let mut keys = BumpVec::with_capacity_in(sort.columns.len(), self.bump);
		for col in sort.columns {
			let expr = self.compile_expr(col.expr, None)?;
			let direction = match col.direction {
				Some(crate::ast::expr::SortDirection::Asc) | None => SortDirection::Asc,
				Some(crate::ast::expr::SortDirection::Desc) => SortDirection::Desc,
			};
			keys.push(SortKey {
				expr,
				direction,
				nulls: NullsOrder::default(),
			});
		}
		Ok(Plan::Sort(SortNode {
			input,
			keys: keys.into_bump_slice(),
			span: sort.span,
		}))
	}

	// ========== Schema-aware compilation methods ==========

	pub(super) fn compile_filter_with_schema(
		&mut self,
		filter: &FilterExpr<'bump>,
		input: &'bump Plan<'bump>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<Plan<'bump>> {
		// Use async version to support subqueries in filter predicates
		let predicate = self.compile_expr_with_subqueries(filter.predicate, schema)?;
		Ok(Plan::Filter(FilterNode {
			input,
			predicate,
			span: filter.span,
		}))
	}

	pub(crate) fn compile_map_with_schema(
		&mut self,
		map: &MapExpr<'bump>,
		input: Option<&'bump Plan<'bump>>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<Plan<'bump>> {
		let mut projections = BumpVec::with_capacity_in(map.projections.len(), self.bump);
		for proj_expr in map.projections {
			projections.push(self.compile_projection(proj_expr, schema)?);
		}
		Ok(Plan::Project(ProjectNode {
			input,
			projections: projections.into_bump_slice(),
			span: map.span,
		}))
	}

	pub(super) fn compile_extend_with_schema(
		&mut self,
		extend: &ExtendExpr<'bump>,
		input: Option<&'bump Plan<'bump>>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<Plan<'bump>> {
		let mut extensions = BumpVec::with_capacity_in(extend.extensions.len(), self.bump);
		for ext_expr in extend.extensions {
			extensions.push(self.compile_projection(ext_expr, schema)?);
		}
		Ok(Plan::Extend(ExtendNode {
			input,
			extensions: extensions.into_bump_slice(),
			span: extend.span,
		}))
	}

	pub(super) fn compile_aggregate_with_schema(
		&mut self,
		agg: &AggregateExpr<'bump>,
		input: &'bump Plan<'bump>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<Plan<'bump>> {
		let group_by = self.compile_expr_slice(agg.group_by, schema)?;
		let mut aggregations = BumpVec::with_capacity_in(agg.aggregations.len(), self.bump);
		for agg_expr in agg.aggregations {
			aggregations.push(self.compile_projection(agg_expr, schema)?);
		}
		Ok(Plan::Aggregate(AggregateNode {
			input,
			group_by,
			aggregations: aggregations.into_bump_slice(),
			span: agg.span,
		}))
	}

	pub(super) fn compile_sort_with_schema(
		&mut self,
		sort: &SortExpr<'bump>,
		input: &'bump Plan<'bump>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<Plan<'bump>> {
		let mut keys = BumpVec::with_capacity_in(sort.columns.len(), self.bump);
		for col in sort.columns {
			let expr = self.compile_expr(col.expr, schema)?;
			let direction = match col.direction {
				Some(crate::ast::expr::SortDirection::Asc) | None => SortDirection::Asc,
				Some(crate::ast::expr::SortDirection::Desc) => SortDirection::Desc,
			};
			keys.push(SortKey {
				expr,
				direction,
				nulls: NullsOrder::default(),
			});
		}
		Ok(Plan::Sort(SortNode {
			input,
			keys: keys.into_bump_slice(),
			span: sort.span,
		}))
	}

	pub(super) fn compile_take(
		&mut self,
		take: &TakeExpr<'bump>,
		input: &'bump Plan<'bump>,
	) -> Result<Plan<'bump>> {
		// Extract count from the expression (should be a literal integer)
		let count = match take.count {
			Expr::Literal(Literal::Integer {
				value,
				..
			}) => value.parse::<u64>().unwrap_or(0),
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("non-literal take count".to_string()),
					span: take.span,
				});
			}
		};
		Ok(Plan::Take(TakeNode {
			input,
			count,
			span: take.span,
		}))
	}

	pub(super) fn compile_distinct(
		&mut self,
		distinct: &DistinctExpr<'bump>,
		input: &'bump Plan<'bump>,
	) -> Result<Plan<'bump>> {
		// For now, distinct on all columns if no columns specified
		// If columns specified, we'd need to resolve them
		let mut columns = BumpVec::new_in(self.bump);
		for col_expr in distinct.columns {
			if let Expr::Identifier(ident) = col_expr {
				let resolved = self.bump.alloc(CatalogColumn {
					id: ColumnId(0),
					name: self.bump.alloc_str(ident.name),
					column_type: Type::Any,
					column_index: 0,
					span: ident.span,
				});
				columns.push(resolved as &'bump CatalogColumn<'bump>);
			}
		}
		Ok(Plan::Distinct(DistinctNode {
			input,
			columns: columns.into_bump_slice(),
			span: distinct.span,
		}))
	}

	/// Extract columns from a Plan (if it's a Scan).
	fn get_plan_columns(&self, plan: &Plan<'bump>) -> Option<&'bump [CatalogColumn<'bump>]> {
		match plan {
			Plan::Scan(scan) => scan.primitive.columns(),
			_ => None,
		}
	}

	/// Build an OutputSchema from a Plan node.
	///
	/// This extracts the available columns from a plan node to enable
	/// column resolution in subsequent pipeline stages.
	pub(super) fn build_schema_from_plan(&self, plan: &Plan<'bump>) -> OutputSchema<'bump> {
		let mut schema = OutputSchema::new_in(self.bump);

		match plan {
			Plan::Scan(scan) => {
				if let Some(columns) = scan.primitive.columns() {
					// Use alias if available, otherwise use table name
					let name = scan.alias.unwrap_or_else(|| scan.primitive.name());
					schema.add_source(name, columns);
				}
			}
			Plan::Filter(filter) => {
				// Filter preserves input schema
				return self.build_schema_from_plan(filter.input);
			}
			Plan::Sort(sort) => {
				// Sort preserves input schema
				return self.build_schema_from_plan(sort.input);
			}
			Plan::Take(take) => {
				// Take preserves input schema
				return self.build_schema_from_plan(take.input);
			}
			Plan::Distinct(distinct) => {
				// Distinct preserves input schema
				return self.build_schema_from_plan(distinct.input);
			}
			Plan::Project(project) => {
				// Project creates a new schema with only the projected columns
				for proj in project.projections.iter() {
					// Use alias if present, otherwise try to extract name from expression
					let name = proj.alias.unwrap_or_else(|| self.extract_expr_name(proj.expr));
					schema.add_computed(name, proj.span);
				}
			}
			Plan::Extend(extend) => {
				// Extend adds columns to input schema
				if let Some(input) = extend.input {
					schema = self.build_schema_from_plan(input);
				}
				// Add each extension as a computed column
				for ext in extend.extensions.iter() {
					let name = ext.alias.unwrap_or_else(|| self.extract_expr_name(ext.expr));
					schema.add_computed(name, ext.span);
				}
			}
			Plan::Aggregate(agg) => {
				// Aggregate creates a new schema with group_by columns and aggregations
				// Group by columns
				for expr in agg.group_by.iter() {
					let name = self.extract_expr_name(expr);
					schema.add_computed(name, expr.span());
				}
				// Aggregation results
				for proj in agg.aggregations.iter() {
					let name = proj.alias.unwrap_or_else(|| self.extract_expr_name(proj.expr));
					schema.add_computed(name, proj.span);
				}
			}
			Plan::JoinInner(join) => {
				// Merge left and right schemas
				schema = self.build_schema_from_plan(join.left);
				let right_schema = self.build_schema_from_plan(join.right);
				schema.merge(&right_schema);
			}
			Plan::JoinLeft(join) => {
				// Merge left and right schemas
				schema = self.build_schema_from_plan(join.left);
				let right_schema = self.build_schema_from_plan(join.right);
				schema.merge(&right_schema);
			}
			Plan::JoinNatural(join) => {
				// Merge left and right schemas
				schema = self.build_schema_from_plan(join.left);
				let right_schema = self.build_schema_from_plan(join.right);
				schema.merge(&right_schema);
			}
			Plan::VariableSource(var_source) => {
				if let Some(stored_schema) = self.get_variable_schema(var_source.variable.variable_id) {
					return stored_schema.clone_schema();
				}
				unimplemented!()
			}
			// For other plan types, return empty schema
			_ => {}
		}

		schema
	}

	/// Extract a column name from a plan expression (for schema building).
	fn extract_expr_name(&self, expr: &PlanExpr<'bump>) -> &'bump str {
		match expr {
			PlanExpr::Column(col) => col.name(),
			PlanExpr::Variable(var) => var.name,
			_ => unreachable!(),
		}
	}

	pub(super) fn compile_join(&mut self, join: &JoinExpr<'bump>, left: &'bump Plan<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::expr::JoinExpr as AstJoin;

		match join {
			AstJoin::Inner(inner) => {
				let right = self.compile_join_source(&inner.source)?;
				let alias = if inner.alias.is_empty() {
					None
				} else {
					Some(inner.alias)
				};
				let right_columns = self.get_plan_columns(right);
				let on =
					self.compile_join_conditions(&inner.using_clause.pairs, alias, right_columns)?;
				let alias = alias.map(|a| self.bump.alloc_str(a) as &'bump str);
				Ok(Plan::JoinInner(JoinInnerNode {
					left,
					right,
					on,
					alias,
					span: inner.span,
				}))
			}
			AstJoin::Left(left_join) => {
				let right = self.compile_join_source(&left_join.source)?;
				let alias = if left_join.alias.is_empty() {
					None
				} else {
					Some(left_join.alias)
				};
				let right_columns = self.get_plan_columns(right);
				let on = self.compile_join_conditions(
					&left_join.using_clause.pairs,
					alias,
					right_columns,
				)?;
				let alias = alias.map(|a| self.bump.alloc_str(a) as &'bump str);
				Ok(Plan::JoinLeft(JoinLeftNode {
					left,
					right,
					on,
					alias,
					span: left_join.span,
				}))
			}
			AstJoin::Natural(natural) => {
				let right = self.compile_join_source(&natural.source)?;
				let alias = if natural.alias.is_empty() {
					None
				} else {
					Some(self.bump.alloc_str(natural.alias) as &'bump str)
				};
				Ok(Plan::JoinNatural(JoinNaturalNode {
					left,
					right,
					join_type: JoinType::Inner,
					alias,
					span: natural.span,
				}))
			}
		}
	}

	/// Compile a join source (subquery or primitive reference).
	fn compile_join_source(&mut self, source: &crate::ast::expr::JoinSource<'bump>) -> Result<&'bump Plan<'bump>> {
		use crate::ast::expr::JoinSource;

		match source {
			JoinSource::SubQuery(expr) => {
				// Need to compile as a pipeline
				let plan = self.compile_pipeline_stage(expr, None)?;
				Ok(self.bump.alloc(plan) as &'bump Plan<'bump>)
			}
			JoinSource::Primitive(prim) => {
				let primitive = self.resolve_primitive(
					prim.source.namespace,
					prim.source.name,
					prim.source.span,
				)?;
				let plan = Plan::Scan(ScanNode {
					primitive,
					alias: prim.source.alias.map(|s| self.bump.alloc_str(s) as &'bump str),
					span: prim.source.span,
				});
				Ok(self.bump.alloc(plan) as &'bump Plan<'bump>)
			}
		}
	}

	/// Compile join conditions from join pairs with alias context for qualified column resolution.
	fn compile_join_conditions(
		&self,
		pairs: &[crate::ast::expr::JoinPair<'bump>],
		right_alias: Option<&str>,
		right_columns: Option<&'bump [CatalogColumn<'bump>]>,
	) -> Result<&'bump [JoinCondition<'bump>]> {
		let mut conditions = BumpVec::with_capacity_in(pairs.len(), self.bump);
		for pair in pairs {
			let left = self.compile_expr_with_aliases(pair.left, right_alias, right_columns)?;
			let right = self.compile_expr_with_aliases(pair.right, right_alias, right_columns)?;
			conditions.push(JoinCondition {
				left,
				right,
			});
		}
		Ok(conditions.into_bump_slice())
	}

	/// Compile an expression with alias context for resolving qualified column references.
	/// Extract identifier name from an expression.
	pub(super) fn compile_merge(
		&mut self,
		merge: &MergeExpr<'bump>,
		left: &'bump Plan<'bump>,
	) -> Result<Plan<'bump>> {
		// Compile the subquery
		let right = self.compile_pipeline_stage(merge.subquery, None)?;
		Ok(Plan::Merge(MergeNode {
			left,
			right: self.bump.alloc(right),
			span: merge.span,
		}))
	}

	pub(super) fn compile_window(
		&mut self,
		window: &WindowExpr<'bump>,
		input: Option<&'bump Plan<'bump>>,
	) -> Result<Plan<'bump>> {
		// Parse window config for type, size, slide
		let mut window_type = WindowType::Tumbling;
		let mut size = WindowSize::Rows(100);
		let mut slide = None;

		for config in window.config {
			match config.key {
				"type" | "kind" => {
					if let Expr::Identifier(ident) = config.value {
						window_type = match ident.name.to_lowercase().as_str() {
							"tumbling" => WindowType::Tumbling,
							"sliding" => WindowType::Sliding,
							"session" => WindowType::Session,
							_ => WindowType::Tumbling,
						};
					}
				}
				"size" | "rows" => {
					if let Expr::Literal(Literal::Integer {
						value,
						..
					}) = config.value
					{
						size = WindowSize::Rows(value.parse::<u64>().unwrap_or(100));
					}
				}
				"slide" => {
					if let Expr::Literal(Literal::Integer {
						value,
						..
					}) = config.value
					{
						slide = Some(WindowSlide::Rows(value.parse::<u64>().unwrap_or(1)));
					}
				}
				_ => {}
			}
		}

		let group_by = self.compile_expr_slice(window.group_by, None)?;
		let mut aggregations = BumpVec::with_capacity_in(window.aggregations.len(), self.bump);
		for agg_expr in window.aggregations {
			aggregations.push(self.compile_projection(agg_expr, None)?);
		}

		Ok(Plan::Window(WindowNode {
			input,
			window_type,
			size,
			slide,
			group_by,
			aggregations: aggregations.into_bump_slice(),
			span: window.span,
		}))
	}
}
