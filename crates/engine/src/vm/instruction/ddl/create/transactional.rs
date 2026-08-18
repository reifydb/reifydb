// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::view::ViewToCreate,
	store::{row_settings::create::create_row_settings, view::create::ViewStorageConfig},
};
use reifydb_core::{
	error::diagnostic::catalog::view_already_exists,
	interface::catalog::{change::CatalogTrackViewChangeOperations, storage::StorageId},
	row::RowSettings,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateTransactionalViewNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{return_error, value::Value};

use super::{create_view_flow, create_view_storage, extract_view_sort};
use crate::{Result, vm::services::Services};

pub(crate) fn create_transactional_view(
	services: &Services,
	txn: &mut AdminTransaction,
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

	let storage = create_view_storage(
		services,
		txn,
		&plan.view,
		plan.namespace.id(),
		&plan.storage_kind,
		&plan.columns,
	)?;

	if let Some(ttl) = &plan.ttl {
		let object_id = match &storage {
			ViewStorageConfig::Table {
				storage,
			} => StorageId::Table(*storage),
			ViewStorageConfig::RingBuffer {
				storage,
				..
			} => StorageId::RingBuffer(*storage),
			ViewStorageConfig::Series {
				storage,
				..
			} => StorageId::Series(*storage),
		};
		create_row_settings(
			txn,
			object_id,
			&RowSettings {
				ttl: Some(ttl.clone()),
				persistent: plan.persistent,
			},
		)?;
	}

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

	create_view_flow(&services.catalog, &services.routines, txn, &result, *plan.as_clause)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id().0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
