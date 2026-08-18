// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::{
		Catalog,
		flow::FlowToCreate,
		ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate},
		series::{SeriesColumnToCreate, SeriesToCreate},
		table::{TableColumnToCreate, TableToCreate},
		view::ViewColumnToCreate,
	},
	store::view::create::ViewStorageConfig,
};
use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		column::ColumnIndex,
		flow::FlowStatus,
		id::NamespaceId,
		view::{View, ViewSortKey},
	},
	sort::SortKey,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::{
	nodes::CompiledViewStorageKind,
	flow::{
		compiler::compile_flow,
		time_domain::{check_join_lateness_requirements, check_window_time_requirements},
	},
	query::QueryPlan,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::fragment::Fragment;

use crate::{Result, vm::services::Services};

fn outermost_sort(plan: &QueryPlan) -> Option<&Vec<SortKey>> {
	match plan {
		QueryPlan::Sort(node) => Some(&node.by),
		QueryPlan::Map(node) => node.input.as_deref().and_then(outermost_sort),
		QueryPlan::Extend(node) => node.input.as_deref().and_then(outermost_sort),
		QueryPlan::Filter(node) => outermost_sort(&node.input),
		QueryPlan::Take(node) => outermost_sort(&node.input),
		QueryPlan::Distinct(node) => outermost_sort(&node.input),
		_ => None,
	}
}

pub(crate) fn extract_view_sort(as_clause: &QueryPlan, columns: &[ViewColumnToCreate]) -> Vec<ViewSortKey> {
	let Some(by) = outermost_sort(as_clause) else {
		return Vec::new();
	};

	let mut resolved = Vec::with_capacity(by.len());
	for key in by {
		let Some(position) = columns.iter().position(|c| c.name.text() == key.column.text()) else {
			return Vec::new();
		};
		resolved.push(ViewSortKey {
			column: ColumnIndex(position as u8),
			direction: key.direction.clone(),
		});
	}
	resolved
}

pub mod authentication;
pub mod binding;
pub mod deferred;
pub mod dictionary;
pub mod event;

pub mod identity;
pub mod identity_attribute;
pub mod migration;
pub mod namespace;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod property;
pub mod queue;
pub mod relationship;
pub mod remote_namespace;
pub mod ringbuffer;
pub mod role;
pub mod series;
pub mod sink;
pub mod source;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod tag;
pub mod test;
pub mod transactional;

pub(crate) fn create_view_flow(
	catalog: &Catalog,
	routines: &Routines,
	txn: &mut AdminTransaction,
	view: &View,
	plan: QueryPlan,
) -> Result<()> {
	let flow = catalog.create_flow(
		txn,
		FlowToCreate {
			name: Fragment::internal(view.name()),
			namespace: view.namespace(),
			status: FlowStatus::Active,
		},
	)?;

	let dag = compile_flow(catalog, routines, txn, plan, Some(view), flow.id)?;
	check_window_time_requirements(catalog, &mut Transaction::Admin(txn), &dag)?;
	check_join_lateness_requirements(catalog, &mut Transaction::Admin(txn), &dag)
}

pub(crate) fn create_view_storage(
	services: &Services,
	txn: &mut AdminTransaction,
	view: &Fragment,
	namespace: NamespaceId,
	storage_kind: &CompiledViewStorageKind,
	columns: &[ViewColumnToCreate],
) -> Result<ViewStorageConfig> {
	let storage_name = Fragment::internal(format!("__view_{}", view.text()));

	match storage_kind {
		CompiledViewStorageKind::Table {
			partition_by,
		} => {
			let columns: Vec<TableColumnToCreate> = columns
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
					name: storage_name,
					namespace,
					columns,
					primary_key_columns: None,
					partition_by: partition_by.clone(),
					underlying: true,
					time: TimeSource::Processing,
				},
			)?;

			Ok(ViewStorageConfig::Table {
				storage: table.id,
			})
		}
		CompiledViewStorageKind::RingBuffer {
			capacity,
			partition_by,
		} => {
			let columns: Vec<RingBufferColumnToCreate> = columns
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
					name: storage_name,
					namespace,
					columns,
					capacity: *capacity,
					partition_by: partition_by.clone(),
					underlying: true,
					time: TimeSource::Processing,
				},
			)?;

			Ok(ViewStorageConfig::RingBuffer {
				storage: ringbuffer.id,
				capacity: *capacity,
			})
		}
		CompiledViewStorageKind::Series {
			key,
			partition_by,
		} => {
			let columns: Vec<SeriesColumnToCreate> = columns
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
					name: storage_name,
					namespace,
					columns,
					tag: None,
					key: key.clone(),
					partition_by: partition_by.clone(),
					underlying: true,
					time: TimeSource::Processing,
				},
			)?;

			Ok(ViewStorageConfig::Series {
				storage: series.id,
				key: key.clone(),
				tag: None,
			})
		}
	}
}
