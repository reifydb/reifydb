// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use thiserror::Error;

use super::{ast::*, token::Span};
use crate::{
	expr::{ColumnRef, ColumnSchema, Expr, Literal, compile_expr, compile_filter},
	operator::{FilterOp, ProjectOp, SelectOp, TakeOp},
	pipeline::Pipeline,
	source::TableSource,
};

/// Compiler error types.
#[derive(Debug, Clone, Error)]
pub enum CompileError {
	#[error("table not found: '{name}'")]
	TableNotFound {
		name: String,
		span: Span,
	},

	#[error("column not found: '{name}' (available: {available:?})")]
	ColumnNotFound {
		name: String,
		available: Vec<String>,
		span: Span,
	},

	#[error("pipeline must start with 'scan'")]
	MustStartWithScan {
		span: Span,
	},

	#[error("invalid take limit: must be positive integer")]
	InvalidTakeLimit {
		span: Span,
	},

	#[error("duplicate column name in extend: '{name}'")]
	DuplicateColumn {
		name: String,
		span: Span,
	},

	#[error("feature not supported: {feature}")]
	NotSupported {
		feature: String,
		span: Span,
	},
}

/// Registry for looking up table sources by name.
pub trait SourceRegistry {
	fn get_source(&self, name: &str) -> Option<Box<dyn TableSource>>;
}

/// Compiler that transforms DSL AST into a Pipeline.
pub struct DslCompiler<'a> {
	sources: &'a dyn SourceRegistry,
}

impl<'a> DslCompiler<'a> {
	/// Create a new compiler with the given source registry.
	pub fn new(sources: &'a dyn SourceRegistry) -> Self {
		Self {
			sources,
		}
	}

	/// Compile a DSL AST into a Pipeline.
	/// For now, only supports single pipeline statements.
	pub fn compile(&self, ast: &DslAst) -> Result<Pipeline, CompileError> {
		// Get the first statement which should be a pipeline
		let stmt = ast.statements.first().ok_or(CompileError::MustStartWithScan {
			span: ast.span,
		})?;

		match stmt {
			crate::dsl::ast::StatementAst::Pipeline(pipeline) => self.compile_pipeline(pipeline),
			_ => Err(CompileError::NotSupported {
				feature: "non-pipeline statements in direct compile".to_string(),
				span: stmt.span(),
			}),
		}
	}

	/// Compile a pipeline AST.
	fn compile_pipeline(&self, ast: &PipelineAst) -> Result<Pipeline, CompileError> {
		// Pipeline must start with scan
		let first_stage = ast.stages.first().ok_or(CompileError::MustStartWithScan {
			span: ast.span,
		})?;

		let (mut pipeline, mut schema) = match first_stage {
			StageAst::Scan(scan) => {
				let source = self.sources.get_source(&scan.table_name).ok_or_else(|| {
					CompileError::TableNotFound {
						name: scan.table_name.clone(),
						span: scan.span,
					}
				})?;

				let schema = source.schema();
				let pipeline = source.scan();
				(pipeline, schema)
			}
			other => {
				return Err(CompileError::MustStartWithScan {
					span: other.span(),
				});
			}
		};

		// Compile remaining stages
		for stage in ast.stages.iter().skip(1) {
			pipeline = self.compile_stage(pipeline, &mut schema, stage)?;
		}

		Ok(pipeline)
	}

	/// Compile a single stage.
	fn compile_stage(
		&self,
		pipeline: Pipeline,
		schema: &mut Vec<ColumnSchema>,
		stage: &StageAst,
	) -> Result<Pipeline, CompileError> {
		match stage {
			StageAst::Scan(_) => {
				// Scan should only be at the start
				Err(CompileError::MustStartWithScan {
					span: stage.span(),
				})
			}

			StageAst::Filter(filter) => {
				let predicate = self.compile_expr(&filter.predicate, schema)?;
				let compiled = compile_filter(predicate);
				Ok(FilterOp::new(compiled).apply(pipeline))
			}

			StageAst::Select(select) => {
				// Validate columns exist and update schema
				let mut new_schema = Vec::new();
				for (new_index, col_name) in select.columns.iter().enumerate() {
					let found = schema.iter().any(|c| c.name == *col_name);
					if !found {
						return Err(CompileError::ColumnNotFound {
							name: col_name.clone(),
							available: schema.iter().map(|c| c.name.clone()).collect(),
							span: select.span,
						});
					}
					new_schema.push(ColumnSchema {
						name: col_name.clone(),
						index: new_index,
					});
				}

				*schema = new_schema;
				Ok(SelectOp::new(select.columns.clone()).apply(pipeline))
			}

			StageAst::Take(take) => Ok(TakeOp::new(take.limit as usize).apply(pipeline)),

			StageAst::Extend(extend) => {
				// Compile extension expressions
				let mut extensions = Vec::new();
				for (name, expr_ast) in &extend.extensions {
					// Check for duplicate column names
					if schema.iter().any(|c| c.name == *name) {
						return Err(CompileError::DuplicateColumn {
							name: name.clone(),
							span: extend.span,
						});
					}

					let expr = self.compile_expr(expr_ast, schema)?;
					let compiled = compile_expr(expr);
					extensions.push((name.clone(), compiled));
				}

				// Update schema with new columns
				for (name, _) in &extensions {
					schema.push(ColumnSchema {
						name: name.clone(),
						index: schema.len(),
					});
				}

				Ok(ProjectOp::extend(extensions).apply(pipeline))
			}

			StageAst::Sort(sort) => {
				// Validate columns exist
				for (col_name, _) in &sort.columns {
					let found = schema.iter().any(|c| c.name == *col_name);
					if !found {
						return Err(CompileError::ColumnNotFound {
							name: col_name.clone(),
							available: schema.iter().map(|c| c.name.clone()).collect(),
							span: sort.span,
						});
					}
				}

				// Convert to SortSpec
				let specs: Vec<crate::operator::SortSpec> = sort
					.columns
					.iter()
					.map(|(name, order)| crate::operator::SortSpec {
						column: name.clone(),
						order: match order {
							SortOrder::Asc => crate::operator::SortOrder::Asc,
							SortOrder::Desc => crate::operator::SortOrder::Desc,
						},
					})
					.collect();

				Ok(crate::operator::SortOp::new(specs).apply(pipeline))
			}
		}
	}

	/// Compile an expression AST to an Expr.
	fn compile_expr(&self, ast: &ExprAst, schema: &[ColumnSchema]) -> Result<Expr, CompileError> {
		match ast {
			ExprAst::Column {
				name,
				span,
			} => {
				let col_schema = schema.iter().find(|c| c.name == *name).ok_or_else(|| {
					CompileError::ColumnNotFound {
						name: name.clone(),
						available: schema.iter().map(|c| c.name.clone()).collect(),
						span: *span,
					}
				})?;

				Ok(Expr::ColumnRef(ColumnRef {
					index: col_schema.index,
					name: name.clone(),
				}))
			}

			ExprAst::Int {
				value,
				..
			} => Ok(Expr::Literal(Literal::Int8(*value))),

			ExprAst::Float {
				value,
				..
			} => Ok(Expr::Literal(Literal::Float8(*value))),

			ExprAst::String {
				value,
				..
			} => Ok(Expr::Literal(Literal::Utf8(value.clone()))),

			ExprAst::Bool {
				value,
				..
			} => Ok(Expr::Literal(Literal::Bool(*value))),

			ExprAst::Null {
				..
			} => Ok(Expr::Literal(Literal::Null)),

			ExprAst::BinaryOp {
				op,
				left,
				right,
				..
			} => {
				let left = Box::new(self.compile_expr(left, schema)?);
				let right = Box::new(self.compile_expr(right, schema)?);
				Ok(Expr::BinaryOp {
					op: *op,
					left,
					right,
				})
			}

			ExprAst::UnaryOp {
				op,
				operand,
				..
			} => {
				let operand = Box::new(self.compile_expr(operand, schema)?);
				Ok(Expr::UnaryOp {
					op: *op,
					operand,
				})
			}

			ExprAst::Paren {
				inner,
				..
			} => self.compile_expr(inner, schema),

			ExprAst::Variable {
				span,
				..
			} => Err(CompileError::NotSupported {
				feature: "variables require bytecode VM".to_string(),
				span: *span,
			}),

			ExprAst::Call {
				span,
				..
			} => Err(CompileError::NotSupported {
				feature: "function calls require bytecode VM".to_string(),
				span: *span,
			}),

			ExprAst::FieldAccess {
				span,
				..
			} => Err(CompileError::NotSupported {
				feature: "field access requires bytecode VM".to_string(),
				span: *span,
			}),

			ExprAst::Subquery {
				span,
				..
			} => Err(CompileError::NotSupported {
				feature: "subqueries require bytecode VM".to_string(),
				span: *span,
			}),

			ExprAst::InList {
				span,
				..
			} => Err(CompileError::NotSupported {
				feature: "IN expressions require bytecode VM".to_string(),
				span: *span,
			}),

			ExprAst::InSubquery {
				span,
				..
			} => Err(CompileError::NotSupported {
				feature: "IN subqueries require bytecode VM".to_string(),
				span: *span,
			}),
		}
	}
}
