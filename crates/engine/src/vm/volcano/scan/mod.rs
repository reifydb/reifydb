// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, ViewId},
			object::ObjectId,
		},
		resolved::ResolvedView,
	},
	value::{
		batch::batch,
		column::{builder::ColumnBuilder, factory::uint16, headers::ColumnHeaders},
	},
};
use reifydb_flow::analyzer::FlowGraphAnalyzer;
use reifydb_transaction::{error::TransactionError, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	value::{
		partition::Partition,
		system_columns::{SystemColumn, with_system_column},
	},
};

use crate::{Result, vm::services::Services};

pub mod column_block_sequence;
pub mod column_predicate;
pub mod column_prune;
pub mod column_series;
pub mod column_table;
pub mod column_unsupported;
pub mod dictionary;
pub mod index;
pub mod queue;
pub mod remote;
pub mod ringbuffer;
pub mod series;
pub mod table;
pub mod view;
pub mod vtable;

pub(crate) fn source_system_columns(partitioned: bool, timed: bool, versioned: bool) -> Vec<SystemColumn> {
	SystemColumn::ALL
		.into_iter()
		.filter(|column| match column {
			SystemColumn::Partitions => partitioned,
			SystemColumn::Time => timed,
			SystemColumn::CommitVersion => versioned,
			SystemColumn::RowNumbers | SystemColumn::CreatedAt | SystemColumn::UpdatedAt => true,
		})
		.collect()
}

pub(crate) fn scan_headers<'a>(names: impl Iterator<Item = &'a str>, system: &[SystemColumn]) -> ColumnHeaders {
	ColumnHeaders {
		columns: names.chain(system.iter().map(|column| column.name())).map(Fragment::internal).collect(),
	}
}

pub(crate) fn empty_scan(user: Vec<(FieldRef, ArrayRef)>, system: &[SystemColumn]) -> Result<RecordBatch> {
	let mut out = batch(user)?;
	for column in system {
		let (_, array) = ColumnBuilder::with_capacity(column.ty(), 0).finish(column.name());
		out = with_system_column(out, *column, array)?;
	}
	Ok(out)
}

pub(crate) fn partition_array(partitions: &[Partition]) -> ArrayRef {
	uint16(SystemColumn::Partitions.name(), partitions.iter().map(|partition| partition.0)).1
}

pub(crate) fn guard_view_read(view: &ResolvedView, rx: &mut Transaction<'_>, services: &Services) -> Result<()> {
	if matches!(rx, Transaction::Test(_)) {
		unimplemented!("RUN TESTS view reads; see plan-operator.md follow-up");
	}
	if !rx.has_unprocessed_flow_changes() {
		return Ok(());
	}
	let upstream = match services.view_lineage.upstream_of(view.def().id()) {
		Some(upstream) => upstream,
		None => match upstream_from_catalog(services, rx, view.def().id())? {
			Some(upstream) => Arc::new(upstream),
			None => return Ok(()),
		},
	};
	let offending: Vec<ObjectId> =
		rx.unprocessed_flow_change_objects().into_iter().filter(|object| upstream.contains(object)).collect();
	if offending.is_empty() {
		return Ok(());
	}
	Err(TransactionError::ViewPendingUpstreamChanges {
		view: view.fully_qualified_name(),
		kind: view.def().kind(),
		upstream: resolve_object_names(services, rx, &offending),
		fragment: view.identifier().clone(),
	}
	.into())
}

fn upstream_from_catalog(
	services: &Services,
	rx: &mut Transaction<'_>,
	view: ViewId,
) -> Result<Option<BTreeSet<ObjectId>>> {
	let mut dags = Vec::new();
	for flow in services.catalog.list_flows_all(rx)? {
		dags.push(services.catalog.get_flow_dag(rx, flow.id)?);
	}
	let mut analyzer = FlowGraphAnalyzer::new();
	analyzer.add_all(dags);
	Ok(analyzer.get_dependency_graph().upstream_closure().remove(&view))
}

fn resolve_object_names(services: &Services, rx: &mut Transaction<'_>, objects: &[ObjectId]) -> Vec<String> {
	let catalog = &services.catalog;
	objects.iter()
		.map(|object| {
			let named = match object {
				ObjectId::Table(id) => catalog
					.find_table(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("table", def.namespace, def.name)),
				ObjectId::View(id) => catalog
					.find_view(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("view", def.namespace(), def.name().to_string())),
				ObjectId::RingBuffer(id) => catalog
					.find_ringbuffer(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("ring buffer", def.namespace, def.name)),
				ObjectId::Series(id) => catalog
					.find_series(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("series", def.namespace, def.name)),
				ObjectId::Dictionary(id) => catalog
					.find_dictionary(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("dictionary", def.namespace, def.name)),
				ObjectId::Queue(id) => catalog
					.find_queue(rx, *id)
					.ok()
					.flatten()
					.map(|def| ("queue", def.namespace, def.name)),
				ObjectId::TableVirtual(_) => None,
			};
			match named {
				Some((kind, namespace, name)) => {
					format!("{} '{}'", kind, qualify(services, rx, namespace, &name))
				}
				None => format!("object {}", object),
			}
		})
		.collect()
}

fn qualify(services: &Services, rx: &mut Transaction<'_>, namespace: NamespaceId, name: &str) -> String {
	match services.catalog.find_namespace(rx, namespace) {
		Ok(Some(namespace)) => format!("{}::{}", namespace.name(), name),
		_ => name.to_string(),
	}
}
