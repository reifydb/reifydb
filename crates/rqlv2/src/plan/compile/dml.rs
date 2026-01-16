// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML operations compilation.

use bumpalo::collections::Vec as BumpVec;
use reifydb_core::interface::catalog::id::ColumnId;
use reifydb_type::value::r#type::Type;

use super::core::{PlanError, PlanErrorKind, Planner, Result};
use crate::{
	ast::stmt::dml::{DeleteStmt, InsertStmt, UpdateStmt},
	plan::{
		Plan,
		node::{
			mutate::*,
			query::{ExtendNode, FilterNode, InlineDataNode, Projection, ScanNode},
		},
		types::{CatalogColumn, Primitive},
	},
};

impl<'bump, 'cat> Planner<'bump, 'cat> {
	pub(super) fn compile_insert(&mut self, insert_stmt: &InsertStmt<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::stmt::dml::InsertSource;

		// Resolve the target
		let primitive = self.resolve_primitive(insert_stmt.namespace, insert_stmt.table, insert_stmt.span)?;
		let target = match primitive {
			Primitive::Table(t) => InsertTarget::Table(t),
			Primitive::RingBuffer(r) => InsertTarget::RingBuffer(r),
			Primitive::Dictionary(d) => InsertTarget::Dictionary(d),
			Primitive::View(_) => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("INSERT into view".to_string()),
					span: insert_stmt.span,
				});
			}
		};

		// Compile the source
		let input = match &insert_stmt.source {
			InsertSource::Values(rows) => {
				// Compile inline values
				let mut compiled_rows = BumpVec::with_capacity_in(rows.len(), self.bump);
				for row in rows.iter() {
					let compiled_row = self.compile_expr_slice(row, None)?;
					compiled_rows.push(compiled_row);
				}
				Plan::InlineData(InlineDataNode {
					rows: compiled_rows.into_bump_slice(),
					span: insert_stmt.span,
				})
			}
			InsertSource::Query(pipeline) => {
				// Compile as pipeline
				let plans = self.compile_statement_body_as_pipeline(pipeline)?;
				if plans.is_empty() {
					return Err(PlanError {
						kind: PlanErrorKind::EmptyPipeline,
						span: insert_stmt.span,
					});
				}
				// Unwrap the plan (it's wrapped in a slice)
				*plans[0]
			}
		};

		// Resolve columns if specified
		let columns = if let Some(col_names) = insert_stmt.columns {
			let mut cols = BumpVec::with_capacity_in(col_names.len(), self.bump);
			for col_name in col_names.iter() {
				// Create placeholder column references
				let col = self.bump.alloc(CatalogColumn {
					id: ColumnId(0),
					name: self.bump.alloc_str(col_name),
					column_type: Type::Any,
					column_index: 0,
					span: insert_stmt.span,
				});
				cols.push(col as &'bump CatalogColumn<'bump>);
			}
			Some(cols.into_bump_slice())
		} else {
			None
		};

		Ok(Plan::Insert(InsertNode {
			target,
			input: self.bump.alloc(input),
			columns,
			span: insert_stmt.span,
		}))
	}
	pub(super) fn compile_update(&mut self, update_stmt: &UpdateStmt<'bump>) -> Result<Plan<'bump>> {
		// Resolve the target
		let primitive = self.resolve_primitive(update_stmt.namespace, update_stmt.table, update_stmt.span)?;
		let target = match primitive {
			Primitive::Table(t) => UpdateTarget::Table(t),
			Primitive::RingBuffer(r) => UpdateTarget::RingBuffer(r),
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("UPDATE on non-table/ringbuffer".to_string()),
					span: update_stmt.span,
				});
			}
		};

		// Build a pipeline: scan -> filter -> map with assignments
		// First, scan the table
		let scan = Plan::Scan(ScanNode {
			primitive,
			alias: None,
			span: update_stmt.span,
		});

		// Apply filter if present
		let filtered = if let Some(filter_expr) = update_stmt.filter {
			let predicate = self.compile_expr(filter_expr, None)?;
			Plan::Filter(FilterNode {
				input: self.bump.alloc(scan),
				predicate,
				span: update_stmt.span,
			})
		} else {
			scan
		};

		// Build the extend node with the assignments
		let mut extensions = BumpVec::with_capacity_in(update_stmt.assignments.len(), self.bump);
		for assign in update_stmt.assignments {
			let value = self.compile_expr(assign.value, None)?;
			extensions.push(Projection {
				expr: value,
				alias: Some(self.bump.alloc_str(assign.column) as &'bump str),
				span: update_stmt.span,
			});
		}

		let extended = Plan::Extend(ExtendNode {
			input: Some(self.bump.alloc(filtered)),
			extensions: extensions.into_bump_slice(),
			span: update_stmt.span,
		});

		Ok(Plan::Update(UpdateNode {
			target,
			input: Some(self.bump.alloc(extended)),
			span: update_stmt.span,
		}))
	}
	pub(super) fn compile_delete(&mut self, delete_stmt: &DeleteStmt<'bump>) -> Result<Plan<'bump>> {
		// Resolve the target
		let primitive = self.resolve_primitive(delete_stmt.namespace, delete_stmt.table, delete_stmt.span)?;
		let target = match primitive {
			Primitive::Table(t) => DeleteTarget::Table(t),
			Primitive::RingBuffer(r) => DeleteTarget::RingBuffer(r),
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("DELETE on non-table/ringbuffer".to_string()),
					span: delete_stmt.span,
				});
			}
		};

		// Build a pipeline: scan -> filter
		let scan = Plan::Scan(ScanNode {
			primitive,
			alias: None,
			span: delete_stmt.span,
		});

		// Apply filter if present
		let input = if let Some(filter_expr) = delete_stmt.filter {
			let predicate = self.compile_expr(filter_expr, None)?;
			Some(self.bump.alloc(Plan::Filter(FilterNode {
				input: self.bump.alloc(scan),
				predicate,
				span: delete_stmt.span,
			})) as &'bump Plan<'bump>)
		} else {
			Some(self.bump.alloc(scan) as &'bump Plan<'bump>)
		};

		Ok(Plan::Delete(DeleteNode {
			target,
			input,
			span: delete_stmt.span,
		}))
	}
}
