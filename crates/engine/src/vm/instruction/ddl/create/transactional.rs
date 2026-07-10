// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::{
		ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate},
		series::{SeriesColumnToCreate, SeriesToCreate},
		table::{TableColumnToCreate, TableToCreate},
		view::ViewToCreate,
	},
	store::{row_settings::create::create_row_settings, view::create::ViewStorageConfig},
};
use reifydb_core::{
	error::diagnostic::catalog::view_already_exists,
	interface::catalog::{change::CatalogTrackViewChangeOperations, shape::ShapeId},
	row::RowSettings,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::{CompiledViewStorageKind, CreateTransactionalViewNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{fragment::Fragment, return_error, value::Value};

use super::{create_deferred_view_flow, extract_view_sort, require_buffer_for_non_persistent};
use crate::{Result, vm::services::Services};

pub(crate) fn create_transactional_view(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateTransactionalViewNode,
) -> Result<Columns> {
	require_buffer_for_non_persistent(txn, plan.persistent, plan.view.clone(), plan.view.text())?;

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

	let storage = create_underlying_primitive(services, txn, &plan)?;

	if let Some(ttl) = &plan.ttl {
		let shape_id = match &storage {
			ViewStorageConfig::Table {
				underlying,
			} => ShapeId::Table(*underlying),
			ViewStorageConfig::RingBuffer {
				underlying,
				..
			} => ShapeId::RingBuffer(*underlying),
			ViewStorageConfig::Series {
				underlying,
				..
			} => ShapeId::Series(*underlying),
		};
		create_row_settings(
			txn,
			shape_id,
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

	create_deferred_view_flow(&services.catalog, &services.routines, txn, &result, *plan.as_clause)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id().0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}

fn create_underlying_primitive(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: &CreateTransactionalViewNode,
) -> Result<ViewStorageConfig> {
	let underlying_name = Fragment::internal(format!("__view_{}", plan.view.text()));
	let namespace = plan.namespace.id();

	match &plan.storage_kind {
		CompiledViewStorageKind::Table {
			partition_by,
		} => {
			let columns: Vec<TableColumnToCreate> = plan
				.columns
				.iter()
				.map(|c| TableColumnToCreate {
					name: c.name.clone(),
					fragment: c.fragment.clone(),
					constraint: c.constraint.clone(),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				})
				.collect();

			let table = services.catalog.create_table(
				txn,
				TableToCreate {
					name: underlying_name,
					namespace,
					columns,
					retention_strategy: None,
					primary_key_columns: None,
					partition_by: partition_by.clone(),
					underlying: true,
				},
			)?;

			Ok(ViewStorageConfig::Table {
				underlying: table.id,
			})
		}
		CompiledViewStorageKind::RingBuffer {
			capacity,
			propagate_evictions,
			partition_by,
		} => {
			let columns: Vec<RingBufferColumnToCreate> = plan
				.columns
				.iter()
				.map(|c| RingBufferColumnToCreate {
					name: c.name.clone(),
					fragment: c.fragment.clone(),
					constraint: c.constraint.clone(),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				})
				.collect();

			let ringbuffer = services.catalog.create_ringbuffer(
				txn,
				RingBufferToCreate {
					name: underlying_name,
					namespace,
					columns,
					capacity: *capacity,
					partition_by: partition_by.clone(),
					underlying: true,
				},
			)?;

			Ok(ViewStorageConfig::RingBuffer {
				underlying: ringbuffer.id,
				capacity: *capacity,
				propagate_evictions: *propagate_evictions,
			})
		}
		CompiledViewStorageKind::Series {
			key,
			partition_by,
		} => {
			let columns: Vec<SeriesColumnToCreate> = plan
				.columns
				.iter()
				.map(|c| SeriesColumnToCreate {
					name: c.name.clone(),
					fragment: c.fragment.clone(),
					constraint: c.constraint.clone(),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				})
				.collect();

			let series = services.catalog.create_series(
				txn,
				SeriesToCreate {
					name: underlying_name,
					namespace,
					columns,
					tag: None,
					key: key.clone(),
					partition_by: partition_by.clone(),
					underlying: true,
				},
			)?;

			Ok(ViewStorageConfig::Series {
				underlying: series.id,
				key: key.clone(),
				tag: None,
			})
		}
	}
}
