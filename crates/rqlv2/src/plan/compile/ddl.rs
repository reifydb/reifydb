// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL operations compilation.

use bumpalo::collections::Vec as BumpVec;

use super::core::{PlanError, PlanErrorKind, Planner, Result};
use crate::{
	ast::stmt::{AlterStmt, CreateStmt, DropStmt},
	plan::{
		Plan, View,
		node::{ddl::*, query::SortDirection},
	},
};

impl<'bump, 'cat> Planner<'bump, 'cat> {
	pub(super) fn compile_create(&mut self, create_stmt: &CreateStmt<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::stmt::ddl::CreateStmt as AstCreate;

		match create_stmt {
			AstCreate::Namespace(ns) => Ok(Plan::Create(CreateNode::Namespace(CreateNamespaceNode {
				name: self.bump.alloc_str(ns.name),
				if_not_exists: ns.if_not_exists,
				span: ns.span,
			}))),

			AstCreate::Table(table) => {
				let namespace = self.resolve_namespace(table.namespace, table.span)?;
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
				let namespace = self.resolve_namespace(view.namespace, view.span)?;
				let query = self.compile_statement_body_as_pipeline(view.query)?;
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
				let table = self.resolve_table(index.namespace, index.table, index.span)?;
				let mut columns = BumpVec::with_capacity_in(index.columns.len(), self.bump);
				for idx_col in index.columns.iter() {
					// Find the column in the table
					let col = table.columns.iter().find(|c| c.name == idx_col.name).ok_or_else(
						|| PlanError {
							kind: PlanErrorKind::ColumnNotFound(idx_col.name.to_string()),
							span: idx_col.span,
						},
					)?;
					columns.push(IndexColumnDef {
						column: col,
						direction: if idx_col.descending {
							SortDirection::Desc
						} else {
							SortDirection::Asc
						},
					});
				}

				Ok(Plan::Create(CreateNode::Index(CreateIndexNode {
					table,
					name: self.bump.alloc_str(index.name),
					columns: columns.into_bump_slice(),
					unique: index.unique,
					filter: None,
					span: index.span,
				})))
			}

			AstCreate::Sequence(seq) => {
				let namespace = self.resolve_namespace(seq.namespace, seq.span)?;

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

			AstCreate::Dictionary(_dict) => Err(PlanError {
				kind: PlanErrorKind::Unsupported("CREATE DICTIONARY not yet implemented".to_string()),
				span: create_stmt.span(),
			}),

			AstCreate::RingBuffer(_rb) => Err(PlanError {
				kind: PlanErrorKind::Unsupported("CREATE RINGBUFFER not yet implemented".to_string()),
				span: create_stmt.span(),
			}),

			AstCreate::Series(_series) => Err(PlanError {
				kind: PlanErrorKind::Unsupported("CREATE SERIES not yet implemented".to_string()),
				span: create_stmt.span(),
			}),

			AstCreate::Subscription(_sub) => Err(PlanError {
				kind: PlanErrorKind::Unsupported("CREATE SUBSCRIPTION not yet implemented".to_string()),
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
	pub(super) fn compile_drop(&mut self, drop_stmt: &DropStmt<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::stmt::ddl::DropObjectType;

		let target = match drop_stmt.object_type {
			DropObjectType::Namespace => DropTarget::Namespace(self.bump.alloc_str(drop_stmt.name)),
			DropObjectType::Table => {
				let table = self.resolve_table(drop_stmt.namespace, drop_stmt.name, drop_stmt.span)?;
				DropTarget::Table(table)
			}
			DropObjectType::View => {
				let ns = self.resolve_namespace(drop_stmt.namespace, drop_stmt.span)?;
				let view_def =
					self.catalog.find_view_by_name(ns.id, drop_stmt.name).ok_or_else(|| {
						PlanError {
							kind: PlanErrorKind::ViewNotFound(drop_stmt.name.to_string()),
							span: drop_stmt.span,
						}
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
			DropObjectType::Dictionary => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("DROP DICTIONARY not yet implemented".to_string()),
					span: drop_stmt.span,
				});
			}
			DropObjectType::RingBuffer => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("DROP RINGBUFFER not yet implemented".to_string()),
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
	pub(super) fn compile_alter(&mut self, alter_stmt: &AlterStmt<'bump>) -> Result<Plan<'bump>> {
		use crate::ast::stmt::ddl::{AlterStmt as AstAlter, AlterTableAction as AstAction};

		match alter_stmt {
			AstAlter::Table(alt) => {
				let table = self.resolve_table(alt.namespace, alt.name, alt.span)?;

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
			AstAlter::View(view) => {
				Err(PlanError {
					kind: PlanErrorKind::Unsupported("ALTER VIEW not yet implemented".to_string()),
					span: view.span,
				})
			}
			AstAlter::Flow(flow) => {
				Err(PlanError {
					kind: PlanErrorKind::Unsupported("ALTER FLOW not yet implemented".to_string()),
					span: flow.span,
				})
			}
		}
	}
}
