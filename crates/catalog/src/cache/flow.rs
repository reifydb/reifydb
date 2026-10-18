// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	flow::dag::FlowDag,
	interface::catalog::{
		flow::{Flow, FlowEntry, FlowId},
		id::NamespaceId,
		view::ViewKind,
	},
};

use crate::cache::{CatalogCache, MultiVersionFlow};

impl CatalogCache {
	pub fn find_flow_entry_at(&self, flow: FlowId, version: CommitVersion) -> Option<FlowEntry> {
		self.flows.get(&flow).and_then(|entry| entry.value().get(version))
	}

	pub fn find_flow_at(&self, flow: FlowId, version: CommitVersion) -> Option<Flow> {
		self.find_flow_entry_at(flow, version).map(|entry| entry.flow)
	}

	pub fn find_flow_by_name_at(&self, namespace: NamespaceId, name: &str, version: CommitVersion) -> Option<Flow> {
		self.flows_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let flow_id = *entry.value();
			self.find_flow_at(flow_id, version)
		})
	}

	pub fn find_flow(&self, flow: FlowId) -> Option<Flow> {
		self.flows.get(&flow).and_then(|entry| entry.value().get_latest()).map(|entry| entry.flow)
	}

	pub fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Flow> {
		self.flows_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let flow_id = *entry.value();
			self.find_flow(flow_id)
		})
	}

	pub fn list_flow_entries_at(&self, version: CommitVersion) -> Vec<FlowEntry> {
		self.flows.iter().filter_map(|entry| entry.value().get(version)).collect()
	}

	pub fn list_all_flows_at(&self, version: CommitVersion) -> Vec<Flow> {
		self.list_flow_entries_at(version).into_iter().map(|entry| entry.flow).collect()
	}

	pub fn transactional_flows_at(&self, version: CommitVersion) -> Arc<[FlowDag]> {
		self.transactional_flows.get(version).unwrap_or_else(|| Arc::from(Vec::new()))
	}

	pub fn refresh_transactional_flows(&self, version: CommitVersion) {
		let flows: Vec<FlowDag> = self
			.flows
			.iter()
			.filter_map(|entry| entry.value().get(version))
			.filter(|entry| {
				entry.dag.sink_views().any(|view| {
					self.find_view_at(view, version)
						.is_some_and(|def| def.kind() == ViewKind::Transactional)
				})
			})
			.map(|entry| entry.dag)
			.collect();
		self.transactional_flows.insert(version, Arc::from(flows));
	}

	pub fn set_flow(&self, id: FlowId, version: CommitVersion, entry: Option<FlowEntry>) {
		let _guard = self.write_lock.lock();
		if let Some(existing) = self.flows.get(&id)
			&& let Some(pre) = existing.value().get_latest()
		{
			self.flows_by_name.remove(&(pre.flow.namespace, pre.flow.name.clone()));
		}

		let multi = self.flows.get_or_insert_with(id, MultiVersionFlow::new);
		if let Some(new) = entry {
			self.flows_by_name.insert((new.flow.namespace, new.flow.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		flow::{
			dag::FlowDag,
			operator::{FlowNode, OperatorDef},
		},
		interface::catalog::{
			flow::{FlowStatus, OperatorId},
			id::ViewId,
			view::{TableView, View, ViewKind},
		},
	};

	use super::*;

	fn view(id: u64, kind: ViewKind) -> View {
		View::Table(TableView {
			id: ViewId(id),
			namespace: NamespaceId::SYSTEM,
			name: format!("view_{id}"),
			kind,
			columns: vec![],
			primary_key: None,
			partition_by: vec![],
			sort: vec![],
		})
	}

	fn sinking_entry(flow: u64, sink: u64) -> FlowEntry {
		let mut builder = FlowDag::builder(FlowId(flow));
		builder.add_node(FlowNode {
			id: OperatorId(1),
			ty: OperatorDef::SinkTableView {
				view: ViewId(sink),
			},
			inputs: vec![],
			outputs: vec![],
		});
		FlowEntry {
			flow: create_test_flow(FlowId(flow), NamespaceId::SYSTEM, &format!("flow_{flow}")),
			dag: builder.build(),
		}
	}

	fn transactional_ids(catalog: &CatalogCache, version: u64) -> Vec<FlowId> {
		catalog.transactional_flows_at(CommitVersion(version)).iter().map(|dag| dag.id).collect()
	}

	#[test]
	fn the_transactional_flow_index_keeps_only_flows_that_sink_into_transactional_views() {
		// a deferred flow in the index makes every write diff for it, a missing transactional flow skips its
		// view.
		let catalog = CatalogCache::new();
		catalog.set_view(ViewId(10), CommitVersion(1), Some(view(10, ViewKind::Deferred)));
		catalog.set_flow(FlowId(1), CommitVersion(1), Some(sinking_entry(1, 10)));
		catalog.refresh_transactional_flows(CommitVersion(1));
		assert_eq!(
			transactional_ids(&catalog, 1),
			Vec::<FlowId>::new(),
			"a deferred-only catalog has no transactional flows"
		);

		catalog.set_view(ViewId(20), CommitVersion(2), Some(view(20, ViewKind::Transactional)));
		catalog.set_flow(FlowId(2), CommitVersion(2), Some(sinking_entry(2, 20)));
		catalog.refresh_transactional_flows(CommitVersion(2));
		assert_eq!(
			transactional_ids(&catalog, 2),
			vec![FlowId(2)],
			"the transactional flow must be listed once created"
		);
	}

	#[test]
	fn the_transactional_flow_index_answers_as_of_each_commit_version() {
		// a write that reads the newest set misses a view created after its snapshot or diffs one already
		// dropped.
		let catalog = CatalogCache::new();
		assert_eq!(
			transactional_ids(&catalog, 1),
			Vec::<FlowId>::new(),
			"before any refresh no flow exists, so the set is empty"
		);

		catalog.set_view(ViewId(20), CommitVersion(2), Some(view(20, ViewKind::Transactional)));
		catalog.set_flow(FlowId(2), CommitVersion(2), Some(sinking_entry(2, 20)));
		catalog.refresh_transactional_flows(CommitVersion(2));
		catalog.set_flow(FlowId(2), CommitVersion(3), None);
		catalog.set_view(ViewId(20), CommitVersion(3), None);
		catalog.refresh_transactional_flows(CommitVersion(3));

		assert_eq!(
			transactional_ids(&catalog, 1),
			Vec::<FlowId>::new(),
			"a version before the create must not see the flow"
		);
		assert_eq!(transactional_ids(&catalog, 2), vec![FlowId(2)], "the create version must see the flow");
		assert_eq!(
			transactional_ids(&catalog, 5),
			Vec::<FlowId>::new(),
			"a version after the drop must not see the flow"
		);
	}

	#[test]
	fn the_transactional_flow_index_does_not_depend_on_view_and_flow_apply_order() {
		// one refresh after the whole commit sees both halves, a refresh between them would see a flow without
		// its view.
		let catalog = CatalogCache::new();
		catalog.set_flow(FlowId(2), CommitVersion(2), Some(sinking_entry(2, 20)));
		catalog.set_view(ViewId(20), CommitVersion(2), Some(view(20, ViewKind::Transactional)));
		catalog.refresh_transactional_flows(CommitVersion(2));
		assert_eq!(transactional_ids(&catalog, 2), vec![FlowId(2)]);
	}

	fn create_test_flow(id: FlowId, namespace: NamespaceId, name: &str) -> Flow {
		Flow {
			id,
			namespace,
			name: name.to_string(),
			status: FlowStatus::Active,
		}
	}

	fn entry(flow: &Flow) -> FlowEntry {
		FlowEntry {
			flow: flow.clone(),
			dag: FlowDag::builder(flow.id).build(),
		}
	}

	#[test]
	fn test_set_and_find_flow() {
		let catalog = CatalogCache::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let flow = create_test_flow(flow_id, namespace_id, "test_flow");

		catalog.set_flow(flow_id, CommitVersion(1), Some(entry(&flow)));

		let found = catalog.find_flow_at(flow_id, CommitVersion(1));
		assert_eq!(found, Some(flow.clone()));

		let found = catalog.find_flow_at(flow_id, CommitVersion(5));
		assert_eq!(found, Some(flow));

		let found = catalog.find_flow_at(flow_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_flow_by_name() {
		let catalog = CatalogCache::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let flow = create_test_flow(flow_id, namespace_id, "named_flow");

		catalog.set_flow(flow_id, CommitVersion(1), Some(entry(&flow)));

		let found = catalog.find_flow_by_name_at(namespace_id, "named_flow", CommitVersion(1));
		assert_eq!(found, Some(flow));

		let found = catalog.find_flow_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		let found = catalog.find_flow_by_name_at(NamespaceId::DEFAULT, "named_flow", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_flow_rename() {
		let catalog = CatalogCache::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let flow_v1 = create_test_flow(flow_id, namespace_id, "old_name");
		catalog.set_flow(flow_id, CommitVersion(1), Some(entry(&flow_v1)));

		assert!(catalog.find_flow_by_name_at(namespace_id, "old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_flow_by_name_at(namespace_id, "new_name", CommitVersion(1)).is_none());

		let mut flow_v2 = flow_v1.clone();
		flow_v2.name = "new_name".to_string();
		catalog.set_flow(flow_id, CommitVersion(2), Some(entry(&flow_v2)));

		assert!(catalog.find_flow_by_name_at(namespace_id, "old_name", CommitVersion(2)).is_none());

		assert_eq!(
			catalog.find_flow_by_name_at(namespace_id, "new_name", CommitVersion(2)),
			Some(flow_v2.clone())
		);

		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(1)), Some(flow_v1));

		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(2)), Some(flow_v2));
	}

	#[test]
	fn test_flow_move_between_namespaces() {
		let catalog = CatalogCache::new();
		let flow_id = FlowId(1);
		let namespace1 = NamespaceId::SYSTEM;
		let namespace2 = NamespaceId::DEFAULT;

		let flow_v1 = create_test_flow(flow_id, namespace1, "movable_flow");
		catalog.set_flow(flow_id, CommitVersion(1), Some(entry(&flow_v1)));

		assert!(catalog.find_flow_by_name_at(namespace1, "movable_flow", CommitVersion(1)).is_some());
		assert!(catalog.find_flow_by_name_at(namespace2, "movable_flow", CommitVersion(1)).is_none());

		let mut flow_v2 = flow_v1.clone();
		flow_v2.namespace = namespace2;
		catalog.set_flow(flow_id, CommitVersion(2), Some(entry(&flow_v2)));

		assert!(catalog.find_flow_by_name_at(namespace1, "movable_flow", CommitVersion(2)).is_none());

		assert!(catalog.find_flow_by_name_at(namespace2, "movable_flow", CommitVersion(2)).is_some());
	}

	#[test]
	fn test_flow_deletion() {
		let catalog = CatalogCache::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let flow = create_test_flow(flow_id, namespace_id, "deletable_flow");
		catalog.set_flow(flow_id, CommitVersion(1), Some(entry(&flow)));

		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(1)), Some(flow.clone()));
		assert!(catalog.find_flow_by_name_at(namespace_id, "deletable_flow", CommitVersion(1)).is_some());

		catalog.set_flow(flow_id, CommitVersion(2), None);

		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(2)), None);
		assert!(catalog.find_flow_by_name_at(namespace_id, "deletable_flow", CommitVersion(2)).is_none());

		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(1)), Some(flow));
	}

	#[test]
	fn test_multiple_flows_in_namespace() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let flow1 = create_test_flow(FlowId(1), namespace_id, "flow1");
		let flow2 = create_test_flow(FlowId(2), namespace_id, "flow2");
		let flow3 = create_test_flow(FlowId(3), namespace_id, "flow3");

		catalog.set_flow(FlowId(1), CommitVersion(1), Some(entry(&flow1)));
		catalog.set_flow(FlowId(2), CommitVersion(1), Some(entry(&flow2)));
		catalog.set_flow(FlowId(3), CommitVersion(1), Some(entry(&flow3)));

		assert_eq!(catalog.find_flow_by_name_at(namespace_id, "flow1", CommitVersion(1)), Some(flow1));
		assert_eq!(catalog.find_flow_by_name_at(namespace_id, "flow2", CommitVersion(1)), Some(flow2));
		assert_eq!(catalog.find_flow_by_name_at(namespace_id, "flow3", CommitVersion(1)), Some(flow3));
	}

	#[test]
	fn test_flow_versioning() {
		let catalog = CatalogCache::new();
		let flow_id = FlowId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let flow_v1 = create_test_flow(flow_id, namespace_id, "flow_v1");
		let mut flow_v2 = flow_v1.clone();
		flow_v2.name = "flow_v2".to_string();
		let mut flow_v3 = flow_v2.clone();
		flow_v3.name = "flow_v3".to_string();

		catalog.set_flow(flow_id, CommitVersion(10), Some(entry(&flow_v1)));
		catalog.set_flow(flow_id, CommitVersion(20), Some(entry(&flow_v2)));
		catalog.set_flow(flow_id, CommitVersion(30), Some(entry(&flow_v3)));

		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(10)), Some(flow_v1.clone()));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(15)), Some(flow_v1));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(20)), Some(flow_v2.clone()));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(25)), Some(flow_v2));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(30)), Some(flow_v3.clone()));
		assert_eq!(catalog.find_flow_at(flow_id, CommitVersion(100)), Some(flow_v3));
	}
}
