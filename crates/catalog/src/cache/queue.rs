// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, QueueId},
		queue::Queue,
	},
};

use crate::cache::{CatalogCache, MultiVersionQueue};

impl CatalogCache {
	pub fn find_queue_at(&self, queue: QueueId, version: CommitVersion) -> Option<Queue> {
		self.queues.get(&queue).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_queue_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Queue> {
		self.queues_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let queue_id = *entry.value();
			self.find_queue_at(queue_id, version)
		})
	}

	pub fn find_queue(&self, queue: QueueId) -> Option<Queue> {
		self.queues.get(&queue).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_queue_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Queue> {
		self.queues_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let queue_id = *entry.value();
			self.find_queue(queue_id)
		})
	}

	pub fn set_queue(&self, id: QueueId, version: CommitVersion, queue: Option<Queue>) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.queues.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.queues_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.queues.get_or_insert_with(id, MultiVersionQueue::new);
		if let Some(new) = queue {
			self.queues_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::queue::{QueueDispatch, QueueRetention, QueueRetry},
	};

	use super::*;

	fn queue(id: QueueId, namespace: NamespaceId, name: &str) -> Queue {
		Queue {
			id,
			namespace,
			name: name.to_string(),
			columns: vec![],
			dispatch: QueueDispatch::Fifo {
				partitions: Queue::DEFAULT_PARTITIONS,
				ordered_by: None,
			},
			retention: QueueRetention::default(),
			retry: QueueRetry::default(),
			underlying: false,
			time: TimeSource::Processing,
			deduplicate: None,
		}
	}

	#[test]
	fn test_set_and_find_queue_at_version() {
		// A queue must be invisible to readers whose snapshot predates its creation,
		// or a DDL leaks into older readers.
		let cache = CatalogCache::new();
		let id = QueueId(1);
		let created = queue(id, NamespaceId::SYSTEM, "jobs");

		cache.set_queue(id, CommitVersion(1), Some(created.clone()));

		assert_eq!(cache.find_queue_at(id, CommitVersion(1)), Some(created.clone()));
		assert_eq!(cache.find_queue_at(id, CommitVersion(5)), Some(created));
		assert_eq!(cache.find_queue_at(id, CommitVersion(0)), None);
	}

	#[test]
	fn test_find_queue_by_name_is_namespace_scoped() {
		let cache = CatalogCache::new();
		let id = QueueId(1);
		let created = queue(id, NamespaceId::SYSTEM, "jobs");

		cache.set_queue(id, CommitVersion(1), Some(created.clone()));

		assert_eq!(cache.find_queue_by_name_at(NamespaceId::SYSTEM, "jobs", CommitVersion(1)), Some(created));
		assert_eq!(cache.find_queue_by_name_at(NamespaceId::SYSTEM, "other", CommitVersion(1)), None);
		assert_eq!(cache.find_queue_by_name_at(NamespaceId::DEFAULT, "jobs", CommitVersion(1)), None);
	}

	#[test]
	fn test_deleted_queue_releases_its_name() {
		// If the name index keeps the name, recreating the queue resolves to the dead definition.
		let cache = CatalogCache::new();
		let id = QueueId(1);
		let created = queue(id, NamespaceId::SYSTEM, "jobs");

		cache.set_queue(id, CommitVersion(1), Some(created.clone()));
		cache.set_queue(id, CommitVersion(2), None);

		assert_eq!(cache.find_queue_at(id, CommitVersion(2)), None);
		assert_eq!(cache.find_queue_by_name_at(NamespaceId::SYSTEM, "jobs", CommitVersion(2)), None);
		assert_eq!(cache.find_queue_at(id, CommitVersion(1)), Some(created));
	}
}
