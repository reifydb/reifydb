// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use reifydb_core::interface::catalog::{id::ViewId, shape::ShapeId};
use reifydb_runtime::sync::rwlock::RwLock;

#[derive(Clone)]
pub struct ViewLineage {
	inner: Arc<RwLock<BTreeMap<ViewId, Arc<BTreeSet<ShapeId>>>>>,
}

impl Default for ViewLineage {
	fn default() -> Self {
		Self {
			inner: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}
}

impl ViewLineage {
	pub fn publish(&self, map: BTreeMap<ViewId, BTreeSet<ShapeId>>) {
		let map = map.into_iter().map(|(view, shapes)| (view, Arc::new(shapes))).collect();
		*self.inner.write() = map;
	}

	pub fn upstream_of(&self, view: ViewId) -> Option<Arc<BTreeSet<ShapeId>>> {
		self.inner.read().get(&view).cloned()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::TableId;

	use super::*;

	#[test]
	fn test_publish_replaces_and_upstream_of_looks_up() {
		let lineage = ViewLineage::default();
		assert!(lineage.upstream_of(ViewId(1)).is_none());

		lineage.publish(BTreeMap::from([(ViewId(1), BTreeSet::from([ShapeId::Table(TableId(9))]))]));
		assert_eq!(*lineage.upstream_of(ViewId(1)).unwrap(), BTreeSet::from([ShapeId::Table(TableId(9))]));

		lineage.publish(BTreeMap::new());
		assert!(lineage.upstream_of(ViewId(1)).is_none(), "publish must replace, not merge");
	}
}
