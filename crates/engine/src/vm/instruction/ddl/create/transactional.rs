// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{catalog::view::ViewToCreate, store::view::create::ViewStorage};
use reifydb_core::{
	error::diagnostic::{catalog::view_already_exists, flow::flow_transactional_not_supported},
	interface::catalog::change::CatalogTrackViewChangeOperations,
	value::column::columns::Columns,
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_rql::nodes::{CompiledViewStorageKind, CreateTransactionalViewNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{error, return_error, value::Value};

use super::{create_transactional_view_flow, extract_view_sort};
use crate::{Result, vm::services::Services};

pub(crate) fn create_transactional_view(
	services: &Services,
	txn: &mut AdminTransaction,
	symbols: &SymbolTable,
	plan: CreateTransactionalViewNode,
) -> Result<Columns> {
	if let Some(view) = services.catalog.find_view_by_name(
		&mut Transaction::Admin(txn),
		plan.namespace.id(),
		plan.view.text(),
	)? {
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("id", Value::Uint8(view.id().0)),
				("namespace", Value::Utf8(plan.namespace.name().to_string())),
				("view", Value::Utf8(plan.view.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}

		return_error!(view_already_exists(plan.view.clone(), plan.namespace.name(), view.name(),));
	}

	if plan.ttl.is_some() {
		return Err(error!(flow_transactional_not_supported("ttl")));
	}

	let storage = match &plan.storage_kind {
		CompiledViewStorageKind::Table {
			partition_by,
		} => ViewStorage::Table {
			partition_by: partition_by.clone(),
		},
		CompiledViewStorageKind::RingBuffer {
			..
		} => return Err(error!(flow_transactional_not_supported("ring buffer storage"))),
		CompiledViewStorageKind::Series {
			..
		} => return Err(error!(flow_transactional_not_supported("series storage"))),
	};

	let sort = extract_view_sort(&plan.as_clause, &plan.columns);

	let result = services.catalog.create_transactional_view(
		txn,
		ViewToCreate {
			name: plan.view.clone(),
			namespace: plan.namespace.id(),
			columns: plan.columns,
			storage,
			sort,
		},
	)?;
	txn.track_view_created(result.clone())?;

	create_transactional_view_flow(
		&services.catalog,
		&services.routines,
		&services.operators,
		txn,
		symbols,
		&result,
		*plan.as_clause,
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id().0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
