// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL operations compilation.

use bumpalo::collections::Vec as BumpVec;
use reifydb_transaction::IntoStandardTransaction;

use super::core::{PlanError, PlanErrorKind, Planner, Result};
use crate::{
	ast::stmt::{AlterStmt, CreateStmt, DropStmt},
	plan::{
		Plan, View,
		node::{ddl::*, query::SortDirection},
	},
};

impl<'bump, 'cat, T: IntoStandardTransaction> Planner<'bump, 'cat, T> {
	pub(super) async fn compile_create(&mut self, create_stmt: &CreateStmt<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::stmt::ddl::CreateStmt as AstCreate;

		match create_stmt {
			AstCreate::Namespace(ns) => Ok(Plan::Create(CreateNode::Namespace(CreateNamespaceNode {
				name: self.bump.alloc_str(ns.name),
				if_not_exists: ns.if_not_exists,
				span: ns.span,
			}))),

			AstCreate::Table(table) => {
				let namespace = self.resolve_namespace(table.namespace, table.span).await?;
				let columns = self.compile_column_definitions(table.columns)?;

				Ok(Plan::Create(CreateNode::Table(CreateTableNode {
					namespace,
					name: self.bump.alloc_str(table.name),
					columns,
					primary_key: None, // TODO: extract from column definitions if specified
					if_not_exists: table.if_not_exists,
					span: table.span,
				})))
			}

			AstCreate::View(view) => {
				let namespace = self.resolve_namespace(view.namespace, view.span).await?;
				let query = self.compile_statement_body_as_pipeline(view.query).await?;
				let query_plan = if query.is_empty() {
					return Err(PlanError {
						kind: PlanErrorKind::EmptyPipeline,
						span: view.span,
					});
				} else {
					query[0]
				};

				Ok(Plan::Create(CreateNode::View(CreateViewNode {
					namespace,
					name: self.bump.alloc_str(view.name),
					query: query_plan,
					if_not_exists: view.if_not_exists,
					span: view.span,
				})))
			}

			AstCreate::Index(index) => {
				let table = self.resolve_table(None, index.table, index.span).await?;
				let mut columns = BumpVec::with_capacity_in(index.columns.len(), self.bump);
				for col_name in index.columns.iter() {
					// Find the column in the table
					let col = table.columns.iter().find(|c| c.name == *col_name).ok_or_else(
						|| PlanError {
							kind: PlanErrorKind::ColumnNotFound(col_name.to_string()),
							span: index.span,
						},
					)?;
					columns.push(IndexColumnDef {
						column: col,
						direction: SortDirection::Asc,
					});
				}

				Ok(Plan::Create(CreateNode::Index(CreateIndexNode {
					table,
					name: self.bump.alloc_str(index.name),
					columns: columns.into_bump_slice(),
					unique: false, // TODO: parse from AST if available
					filter: None,
					span: index.span,
				})))
			}

			AstCreate::Sequence(seq) => {
				let namespace = self.resolve_namespace(seq.namespace, seq.span).await?;

				Ok(Plan::Create(CreateNode::Sequence(CreateSequenceNode {
					namespace,
					name: self.bump.alloc_str(seq.name),
					start: seq.start,
					increment: seq.increment,
					if_not_exists: seq.if_not_exists,
					span: seq.span,
				})))
			}

			AstCreate::Flow(_flow) => Err(PlanError {
				kind: PlanErrorKind::Unsupported("CREATE FLOW not yet implemented".to_string()),
				span: create_stmt.span(),
			}),
		}
	}
	/// Compile column definitions for CREATE TABLE.

	fn compile_column_definitions(
		&self,
		columns: &[crate::ast::stmt::ddl::ColumnDef<'bump>],
	) -> Result<&'bump [ColumnDefinition<'bump>]> {
		let mut defs = BumpVec::with_capacity_in(columns.len(), self.bump);
		for col in columns {
			let column_type = self.parse_type_from_string(col.data_type)?;
			let default = if let Some(def_expr) = col.default {
				Some(self.compile_expr(def_expr, None)?)
			} else {
				None
			};

			defs.push(ColumnDefinition {
				name: self.bump.alloc_str(col.name),
				column_type,
				nullable: col.nullable,
				default,
				span: col.span,
			});
		}
		Ok(defs.into_bump_slice())
	}
	pub(super) async fn compile_drop(&mut self, drop_stmt: &DropStmt<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::stmt::ddl::DropObjectType;

		let target = match drop_stmt.object_type {
			DropObjectType::Namespace => DropTarget::Namespace(self.bump.alloc_str(drop_stmt.name)),
			DropObjectType::Table => {
				let table =
					self.resolve_table(drop_stmt.namespace, drop_stmt.name, drop_stmt.span).await?;
				DropTarget::Table(table)
			}
			DropObjectType::View => {
				let ns = self.resolve_namespace(drop_stmt.namespace, drop_stmt.span).await?;
				let view_def = self
					.catalog
					.find_view_by_name(self.tx, ns.id, drop_stmt.name)
					.await
					.map_err(|e| PlanError {
						kind: PlanErrorKind::Unsupported(format!("catalog error: {}", e)),
						span: drop_stmt.span,
					})?
					.ok_or_else(|| PlanError {
						kind: PlanErrorKind::ViewNotFound(drop_stmt.name.to_string()),
						span: drop_stmt.span,
					})?;

				let columns = self.resolve_columns(&view_def.columns, drop_stmt.span);
				let view = self.bump.alloc(View {
					id: view_def.id,
					namespace: ns,
					name: self.bump.alloc_str(&view_def.name),
					columns,
					span: drop_stmt.span,
				});
				DropTarget::View(view)
			}
			DropObjectType::Index => {
				// For now, return unsupported since we need index resolution
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("DROP INDEX resolution".to_string()),
					span: drop_stmt.span,
				});
			}
			DropObjectType::Sequence => {
				// For now, return unsupported since we need sequence resolution
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("DROP SEQUENCE resolution".to_string()),
					span: drop_stmt.span,
				});
			}
			DropObjectType::Flow => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("DROP FLOW not yet implemented".to_string()),
					span: drop_stmt.span,
				});
			}
		};

		Ok(Plan::Drop(DropNode {
			target,
			if_exists: drop_stmt.if_exists,
			span: drop_stmt.span,
		}))
	}
	pub(super) async fn compile_alter(&mut self, alter_stmt: &AlterStmt<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::stmt::ddl::{AlterStmt as AstAlter, AlterTableAction as AstAction};

		match alter_stmt {
			AstAlter::Table(alt) => {
				let table = self.resolve_table(alt.namespace, alt.name, alt.span).await?;

				let action = match &alt.action {
					AstAction::AddColumn(col_def) => {
						let column_type = self.parse_type_from_string(col_def.data_type)?;
						let default = if let Some(def_expr) = col_def.default {
							Some(self.compile_expr(def_expr, None)?)
						} else {
							None
						};

						AlterTableAction::AddColumn(ColumnDefinition {
							name: self.bump.alloc_str(col_def.name),
							column_type,
							nullable: col_def.nullable,
							default,
							span: col_def.span,
						})
					}
					AstAction::DropColumn(col_name) => {
						AlterTableAction::DropColumn(self.bump.alloc_str(col_name))
					}
					AstAction::RenameColumn {
						from,
						to,
					} => AlterTableAction::RenameColumn {
						from: self.bump.alloc_str(from),
						to: self.bump.alloc_str(to),
					},
				};

				Ok(Plan::Alter(AlterNode::Table(AlterTableNode {
					table,
					action,
					span: alt.span,
				})))
			}
			AstAlter::Sequence(seq) => {
				// For now, return unsupported since we need sequence resolution
				Err(PlanError {
					kind: PlanErrorKind::Unsupported("ALTER SEQUENCE resolution".to_string()),
					span: seq.span,
				})
			}
		}
	}
}
