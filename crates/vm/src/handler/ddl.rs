// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL opcodes: CreateNamespace, CreateTable, DropObject.

use std::str::FromStr;

use reifydb_catalog::catalog::{
	namespace::NamespaceToCreate,
	table::{TableColumnToCreate, TableToCreate},
};
use reifydb_core::value::column::columns::Columns;
use reifydb_rqlv2::bytecode::{opcode::ObjectType, program::DdlDef};
use reifydb_type::{
	fragment::Fragment,
	value::{Value, constraint::TypeConstraint, r#type::Type},
};

use super::HandlerContext;
use crate::{
	error::{Result, VmError},
	runtime::{dispatch::DispatchResult, operand::OperandValue},
};

/// CreateNamespace - create a new namespace.
pub fn create_namespace(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let def_index = ctx.read_u16()?;

	let def = ctx.vm.program.ddl_defs.get(def_index as usize).ok_or(VmError::InvalidDdlDefIndex {
		index: def_index,
	})?;

	if let DdlDef::CreateNamespace(ns_def) = def {
		let ns_def = ns_def.clone();
		let catalog = ctx.vm.context.catalog.as_ref().ok_or(VmError::UnsupportedOperation {
			operation: "Operation requires a catalog".into(),
		})?;
		let tx = ctx.tx.as_mut().ok_or(VmError::TransactionRequired)?;
		let cmd_tx = tx.admin_mut();

		// Check if namespace already exists
		if let Some(_) =
			catalog.find_namespace_by_name(cmd_tx, &ns_def.name).map_err(|e| VmError::CatalogError {
				message: e.to_string(),
			})? {
			if ns_def.if_not_exists {
				// Return success with created=false
				let result = Columns::single_row([
					("namespace", Value::Utf8(ns_def.name.clone())),
					("created", Value::Boolean(false)),
				]);
				ctx.vm.push_operand(OperandValue::Frame(result))?;
				return Ok(ctx.advance_and_continue());
			}
			return Err(VmError::CatalogError {
				message: format!("Namespace '{}' already exists", ns_def.name),
			});
		}

		// Create the namespace (tracking is done internally by catalog.create_namespace)
		let result = catalog
			.create_namespace(
				cmd_tx,
				NamespaceToCreate {
					namespace_fragment: Some(Fragment::internal(ns_def.name.clone())),
					name: ns_def.name.clone(),
				},
			)
			.map_err(|e| VmError::CatalogError {
				message: e.to_string(),
			})?;

		let columns = Columns::single_row([
			("namespace", Value::Utf8(result.name)),
			("created", Value::Boolean(true)),
		]);
		ctx.vm.push_operand(OperandValue::Frame(columns))?;
	} else {
		return Err(VmError::UnexpectedDdlType {
			expected: "CreateNamespace".into(),
			found: format!("{:?}", def),
		});
	}

	Ok(ctx.advance_and_continue())
}

/// CreateTable - create a new table.
pub fn create_table(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let def_index = ctx.read_u16()?;

	let def = ctx.vm.program.ddl_defs.get(def_index as usize).ok_or(VmError::InvalidDdlDefIndex {
		index: def_index,
	})?;

	if let DdlDef::CreateTable(table_def) = def {
		let table_def = table_def.clone();
		let catalog = ctx.vm.context.catalog.as_ref().ok_or(VmError::UnsupportedOperation {
			operation: "Operation requires a catalog".into(),
		})?;
		let tx = ctx.tx.as_mut().ok_or(VmError::TransactionRequired)?;
		let cmd_tx = tx.admin_mut();

		// Get namespace
		let namespace_name = table_def.namespace.as_deref().unwrap_or("default");
		let namespace = catalog
			.find_namespace_by_name(cmd_tx, namespace_name)
			.map_err(|e| VmError::CatalogError {
				message: e.to_string(),
			})?
			.ok_or_else(|| VmError::CatalogError {
				message: format!("Namespace '{}' not found", namespace_name),
			})?;

		// Check if table already exists
		if let Some(_) = catalog.find_table_by_name(cmd_tx, namespace.id, &table_def.name).map_err(|e| {
			VmError::CatalogError {
				message: e.to_string(),
			}
		})? {
			if table_def.if_not_exists {
				let result = Columns::single_row([
					("namespace", Value::Utf8(namespace_name.to_string())),
					("table", Value::Utf8(table_def.name.clone())),
					("created", Value::Boolean(false)),
				]);
				ctx.vm.push_operand(OperandValue::Frame(result))?;
				return Ok(ctx.advance_and_continue());
			}
			return Err(VmError::CatalogError {
				message: format!(
					"Table '{}' already exists in namespace '{}'",
					table_def.name, namespace_name
				),
			});
		}

		// Convert column definitions
		let columns: Vec<TableColumnToCreate> = table_def
			.columns
			.iter()
			.map(|col| {
				let data_type = Type::from_str(&col.data_type).unwrap_or(Type::Any);
				TableColumnToCreate {
					name: col.name.clone(),
					constraint: TypeConstraint::unconstrained(data_type),
					policies: vec![],
					auto_increment: false,
					fragment: None,
					dictionary_id: None,
				}
			})
			.collect();

		// Create the table (tracking is done internally by catalog.create_table)
		let table = catalog
			.create_table(
				cmd_tx,
				TableToCreate {
					fragment: Some(Fragment::internal(table_def.name.clone())),
					table: table_def.name.clone(),
					namespace: namespace.id,
					columns,
					retention_policy: None,
					primary_key_columns: None,
				},
			)
			.map_err(|e| VmError::CatalogError {
				message: e.to_string(),
			})?;

		let result = Columns::single_row([
			("namespace", Value::Utf8(namespace_name.to_string())),
			("table", Value::Utf8(table.name)),
			("created", Value::Boolean(true)),
		]);
		ctx.vm.push_operand(OperandValue::Frame(result))?;
	} else {
		return Err(VmError::UnexpectedDdlType {
			expected: "CreateTable".into(),
			found: format!("{:?}", def),
		});
	}

	Ok(ctx.advance_and_continue())
}

/// DropObject - drop a table, namespace, or other object.
pub fn drop_object(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let def_index = ctx.read_u16()?;
	let _object_type = ctx.read_u8()?;

	let def = ctx.vm.program.ddl_defs.get(def_index as usize).ok_or(VmError::InvalidDdlDefIndex {
		index: def_index,
	})?;

	if let DdlDef::Drop(drop_def) = def {
		let drop_def = drop_def.clone();
		let catalog = ctx.vm.context.catalog.as_ref().ok_or(VmError::UnsupportedOperation {
			operation: "Operation requires a catalog".into(),
		})?;
		let tx = ctx.tx.as_mut().ok_or(VmError::TransactionRequired)?;
		let cmd_tx = tx.admin_mut();

		match drop_def.object_type {
			ObjectType::Table => {
				// Parse namespace.table from name
				let parts: Vec<&str> = drop_def.name.split('.').collect();
				let (namespace_name, table_name) = if parts.len() >= 2 {
					(parts[0], parts[1])
				} else {
					("default", parts[0])
				};

				let namespace =
					catalog.find_namespace_by_name(cmd_tx, namespace_name).map_err(|e| {
						VmError::CatalogError {
							message: e.to_string(),
						}
					})?;

				if let Some(ns) = namespace {
					let table =
						catalog.find_table_by_name(cmd_tx, ns.id, table_name).map_err(|e| {
							VmError::CatalogError {
								message: e.to_string(),
							}
						})?;

					if let Some(t) = table {
						// Delete table (tracking is done internally by catalog.delete_table)
						catalog.delete_table(cmd_tx, t.clone()).map_err(|e| {
							VmError::CatalogError {
								message: e.to_string(),
							}
						})?;

						let result = Columns::single_row([
							("object_type", Value::Utf8("table".to_string())),
							("name", Value::Utf8(drop_def.name.clone())),
							("dropped", Value::Boolean(true)),
						]);
						ctx.vm.push_operand(OperandValue::Frame(result))?;
					} else if !drop_def.if_exists {
						return Err(VmError::CatalogError {
							message: format!("Table '{}' not found", drop_def.name),
						});
					} else {
						let result = Columns::single_row([
							("object_type", Value::Utf8("table".to_string())),
							("name", Value::Utf8(drop_def.name.clone())),
							("dropped", Value::Boolean(false)),
						]);
						ctx.vm.push_operand(OperandValue::Frame(result))?;
					}
				} else if !drop_def.if_exists {
					return Err(VmError::CatalogError {
						message: format!("Namespace '{}' not found", namespace_name),
					});
				} else {
					let result = Columns::single_row([
						("object_type", Value::Utf8("table".to_string())),
						("name", Value::Utf8(drop_def.name.clone())),
						("dropped", Value::Boolean(false)),
					]);
					ctx.vm.push_operand(OperandValue::Frame(result))?;
				}
			}
			ObjectType::Namespace => {
				let namespace =
					catalog.find_namespace_by_name(cmd_tx, &drop_def.name).map_err(|e| {
						VmError::CatalogError {
							message: e.to_string(),
						}
					})?;

				if let Some(ns) = namespace {
					// Delete namespace (tracking is done internally by catalog.delete_namespace)
					catalog.delete_namespace(cmd_tx, ns.clone()).map_err(|e| {
						VmError::CatalogError {
							message: e.to_string(),
						}
					})?;

					let result = Columns::single_row([
						("object_type", Value::Utf8("namespace".to_string())),
						("name", Value::Utf8(drop_def.name.clone())),
						("dropped", Value::Boolean(true)),
					]);
					ctx.vm.push_operand(OperandValue::Frame(result))?;
				} else if !drop_def.if_exists {
					return Err(VmError::CatalogError {
						message: format!("Namespace '{}' not found", drop_def.name),
					});
				} else {
					let result = Columns::single_row([
						("object_type", Value::Utf8("namespace".to_string())),
						("name", Value::Utf8(drop_def.name.clone())),
						("dropped", Value::Boolean(false)),
					]);
					ctx.vm.push_operand(OperandValue::Frame(result))?;
				}
			}
			_ => {
				return Err(VmError::UnsupportedOperation {
					operation: format!("DROP {:?} not yet implemented", drop_def.object_type),
				});
			}
		}
	} else {
		return Err(VmError::UnexpectedDdlType {
			expected: "Drop".into(),
			found: format!("{:?}", def),
		});
	}

	Ok(ctx.advance_and_continue())
}

/// Unsupported DDL operations (CreateView, CreateIndex, etc.)
pub fn unsupported_ddl(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	Err(VmError::UnsupportedOperation {
		operation: format!("DDL opcode at position {} not yet implemented", ctx.vm.ip),
	})
}
