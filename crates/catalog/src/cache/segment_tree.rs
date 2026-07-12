// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, SegmentTreeId},
		segment_tree::SegmentTree,
	},
};

use crate::cache::{CatalogCache, MultiVersionSegmentTree};

impl CatalogCache {
	pub fn find_segment_tree_at(&self, segment_tree: SegmentTreeId, version: CommitVersion) -> Option<SegmentTree> {
		self.segment_trees.get(&segment_tree).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_segment_tree_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<SegmentTree> {
		self.segment_trees_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let segment_tree_id = *entry.value();
			self.find_segment_tree_at(segment_tree_id, version)
		})
	}

	pub fn find_segment_tree(&self, segment_tree: SegmentTreeId) -> Option<SegmentTree> {
		self.segment_trees.get(&segment_tree).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_segment_tree_by_name(&self, namespace: NamespaceId, name: &str) -> Option<SegmentTree> {
		self.segment_trees_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let segment_tree_id = *entry.value();
			self.find_segment_tree(segment_tree_id)
		})
	}

	pub fn list_segment_tree(&self) -> Vec<SegmentTree> {
		self.segment_trees.iter().filter_map(|entry| entry.value().get_latest()).collect()
	}

	pub fn set_segment_tree(&self, id: SegmentTreeId, version: CommitVersion, segment_tree: Option<SegmentTree>) {
		if let Some(entry) = self.segment_trees.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.segment_trees_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.segment_trees.get_or_insert_with(id, MultiVersionSegmentTree::new);
		if let Some(new) = segment_tree {
			self.segment_trees_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}
