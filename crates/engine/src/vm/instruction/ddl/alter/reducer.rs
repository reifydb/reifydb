// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::reducer::ReducerActionDef, value::column::columns::Columns};
use reifydb_rql::nodes::{AlterReducerAction, AlterReducerNode};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{Value, blob::Blob};

use crate::vm::services::Services;

pub(crate) fn execute_alter_reducer(
	services: &Services,
	txn: &mut AdminTransaction,
	node: AlterReducerNode,
) -> crate::Result<Columns> {
	// Resolve reducer by namespace + name
	let namespace_name = &node.namespace.name;
	let reducer_name = node.reducer.text();

	let reducer_def =
		services.catalog.find_reducer_by_name(txn, node.namespace.id, reducer_name)?.ok_or_else(|| {
			reifydb_type::error::Error(reifydb_core::error::diagnostic::internal::internal_with_context(
				&format!("Reducer '{}' not found in namespace '{}'", reducer_name, namespace_name),
				file!(),
				line!(),
				column!(),
				module_path!(),
				module_path!(),
			))
		})?;

	match node.action {
		AlterReducerAction::AddAction {
			name,
			columns: _columns,
			on_dispatch: _on_dispatch,
		} => {
			let action_name = name.text().to_string();

			// Check if action already exists
			if services.catalog.find_reducer_action_by_name(txn, reducer_def.id, &action_name)?.is_some() {
				return Ok(Columns::single_row([
					("reducer", Value::Utf8(reducer_name.to_string())),
					("action", Value::Utf8(action_name)),
					("result", Value::Utf8("already_exists".to_string())),
				]));
			}

			let action_id = services.catalog.next_reducer_action_id(txn)?;

			// Store the on_dispatch pipeline as a blob
			// Full deserialization/execution is handled by DISPATCH (follow-up)
			let data = Blob::new(Vec::new());

			let action_def = ReducerActionDef {
				id: action_id,
				reducer: reducer_def.id,
				name: action_name.clone(),
				data,
			};

			services.catalog.create_reducer_action(txn, &action_def)?;

			Ok(Columns::single_row([
				("reducer", Value::Utf8(reducer_name.to_string())),
				("action", Value::Utf8(action_name)),
				("result", Value::Utf8("added".to_string())),
			]))
		}
		AlterReducerAction::AlterAction {
			name,
			on_dispatch: _on_dispatch,
		} => {
			let action_name = name.text().to_string();

			let existing_action =
				services.catalog
					.find_reducer_action_by_name(txn, reducer_def.id, &action_name)?
					.ok_or_else(|| {
						reifydb_type::error::Error(reifydb_core::error::diagnostic::internal::internal_with_context(
						&format!("Action '{}' not found on reducer '{}'", action_name, reducer_name),
						file!(),
						line!(),
						column!(),
						module_path!(),
						module_path!(),
					))
					})?;

			// Delete old action and recreate with new data
			services.catalog.delete_reducer_action(txn, &existing_action)?;

			let data = Blob::new(Vec::new());
			let updated_action = ReducerActionDef {
				id: existing_action.id,
				reducer: reducer_def.id,
				name: action_name.clone(),
				data,
			};

			services.catalog.create_reducer_action(txn, &updated_action)?;

			Ok(Columns::single_row([
				("reducer", Value::Utf8(reducer_name.to_string())),
				("action", Value::Utf8(action_name)),
				("result", Value::Utf8("altered".to_string())),
			]))
		}
		AlterReducerAction::DropAction {
			name,
		} => {
			let action_name = name.text().to_string();

			let existing_action =
				services.catalog
					.find_reducer_action_by_name(txn, reducer_def.id, &action_name)?
					.ok_or_else(|| {
						reifydb_type::error::Error(reifydb_core::error::diagnostic::internal::internal_with_context(
						&format!("Action '{}' not found on reducer '{}'", action_name, reducer_name),
						file!(),
						line!(),
						column!(),
						module_path!(),
						module_path!(),
					))
					})?;

			services.catalog.delete_reducer_action(txn, &existing_action)?;

			Ok(Columns::single_row([
				("reducer", Value::Utf8(reducer_name.to_string())),
				("action", Value::Utf8(action_name)),
				("result", Value::Utf8("dropped".to_string())),
			]))
		}
	}
}
