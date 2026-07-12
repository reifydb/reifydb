// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet, VecDeque};

use postcard::from_bytes;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	error::diagnostic::catalog::update_partition_column_immutable,
	interface::catalog::id::{RingBufferId, SeriesId, TableId, ViewId},
	internal_error,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::return_error;

use crate::{Result, expression::IdentExpression, flow::node::FlowNodeType};

pub(crate) enum UpdateTarget {
	Table(TableId),
	RingBuffer(RingBufferId),
	Series(SeriesId),
}

impl UpdateTarget {
	fn matches_source(&self, ty: &FlowNodeType) -> bool {
		match (self, ty) {
			(
				UpdateTarget::Table(id),
				FlowNodeType::SourceTable {
					table,
				},
			) => table == id,
			(
				UpdateTarget::RingBuffer(id),
				FlowNodeType::SourceRingBuffer {
					ringbuffer,
				},
			) => ringbuffer == id,
			(
				UpdateTarget::Series(id),
				FlowNodeType::SourceSeries {
					series,
				},
			) => series == id,
			_ => false,
		}
	}
}

pub(crate) fn check_partition_immutability(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target_namespace: &str,
	target_name: &str,
	target: UpdateTarget,
	assigned_columns: &[IdentExpression],
) -> Result<()> {
	if assigned_columns.is_empty() {
		return Ok(());
	}

	let forbidden = downstream_view_partition_columns(catalog, tx, target)?;
	if forbidden.is_empty() {
		return Ok(());
	}

	for assigned in assigned_columns {
		let column_name = assigned.0.text();
		let Some(view_id) = forbidden.get(column_name) else {
			continue;
		};
		let view = catalog.get_view(tx, *view_id)?;
		let view_namespace = catalog
			.find_namespace(tx, view.namespace())?
			.map(|n| n.name().to_string())
			.unwrap_or_else(|| "?".to_string());
		return_error!(update_partition_column_immutable(
			assigned.0.clone(),
			column_name,
			target_namespace,
			target_name,
			Some((view_namespace.as_str(), view.name())),
		));
	}
	Ok(())
}

fn downstream_view_partition_columns(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target: UpdateTarget,
) -> Result<HashMap<String, ViewId>> {
	let mut forbidden: HashMap<String, ViewId> = HashMap::new();

	let nodes = catalog.list_flow_nodes_all(tx)?;
	let mut decoded = Vec::with_capacity(nodes.len());
	for node in &nodes {
		let ty: FlowNodeType = from_bytes(node.data.as_ref())
			.map_err(|e| internal_error!("Failed to deserialize flow node type: {}", e))?;
		decoded.push((node.flow, ty));
	}

	let mut worklist: VecDeque<Anchor> = VecDeque::from([Anchor::Target(target)]);
	let mut visited_views: HashSet<ViewId> = HashSet::new();

	while let Some(anchor) = worklist.pop_front() {
		let matched_flows: HashSet<_> =
			decoded.iter().filter(|(_, ty)| anchor.matches_source(ty)).map(|(flow, _)| *flow).collect();

		for flow_id in matched_flows {
			for (node_flow, ty) in &decoded {
				if *node_flow != flow_id {
					continue;
				}
				let (view, partition_by) = match ty {
					FlowNodeType::SinkTableView {
						view,
						table,
					} => (*view, catalog.get_table(tx, *table)?.partition_by),
					FlowNodeType::SinkRingBufferView {
						view,
						ringbuffer,
						..
					} => (*view, catalog.get_ringbuffer(tx, *ringbuffer)?.partition_by),
					FlowNodeType::SinkSeriesView {
						view,
						series,
						..
					} => (*view, catalog.get_series(tx, *series)?.partition_by),
					_ => continue,
				};
				for column in partition_by {
					forbidden.entry(column).or_insert(view);
				}
				if visited_views.insert(view) {
					worklist.push_back(Anchor::View(view));
				}
			}
		}
	}

	Ok(forbidden)
}

enum Anchor {
	Target(UpdateTarget),
	View(ViewId),
}

impl Anchor {
	fn matches_source(&self, ty: &FlowNodeType) -> bool {
		match self {
			Anchor::Target(target) => target.matches_source(ty),
			Anchor::View(id) => matches!(ty, FlowNodeType::SourceView { view } if view == id),
		}
	}
}
