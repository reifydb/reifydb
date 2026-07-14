// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		handler::Handler,
		id::{HandlerId, NamespaceId},
	},
};
use reifydb_value::value::sumtype::VariantRef;

use crate::cache::{CatalogCache, MultiVersionHandler};

impl CatalogCache {
	pub fn find_handler_at(&self, handler: HandlerId, version: CommitVersion) -> Option<Handler> {
		self.handlers.get(&handler).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_handler_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Handler> {
		self.handlers_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let handler_id = *entry.value();
			self.find_handler_at(handler_id, version)
		})
	}

	pub fn list_handlers_for_variant_at(&self, variant: VariantRef, version: CommitVersion) -> Vec<Handler> {
		if let Some(entry) = self.handlers_by_variant.get(&variant) {
			entry.value().iter().filter_map(|id| self.find_handler_at(*id, version)).collect()
		} else {
			vec![]
		}
	}

	pub fn set_handler(&self, id: HandlerId, version: CommitVersion, handler: Option<Handler>) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.handlers.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.handlers_by_name.remove(&(pre.namespace, pre.name.clone()));

			if let Some(ids_entry) = self.handlers_by_variant.get(&pre.variant) {
				let mut ids = ids_entry.value().clone();
				ids.retain(|existing| *existing != id);
				drop(ids_entry);
				self.handlers_by_variant.insert(pre.variant, ids);
			}
		}

		let multi = self.handlers.get_or_insert_with(id, MultiVersionHandler::new);
		if let Some(new) = handler {
			self.handlers_by_name.insert((new.namespace, new.name.clone()), id);

			if let Some(entry) = self.handlers_by_variant.get(&new.variant) {
				let mut ids = entry.value().clone();
				if !ids.contains(&id) {
					ids.push(id);
				}
				drop(entry);
				self.handlers_by_variant.insert(new.variant, ids);
			} else {
				self.handlers_by_variant.insert(new.variant, vec![id]);
			}

			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
mod tests {
	use std::{
		sync::{Arc, Barrier},
		thread,
	};

	use reifydb_value::value::sumtype::SumTypeId;

	use super::*;

	fn handler(id: u64, namespace: u64, variant: VariantRef) -> Handler {
		Handler {
			id: HandlerId(id),
			namespace: NamespaceId(namespace),
			name: format!("handler_{id}"),
			variant,
			body_source: String::new(),
		}
	}

	#[test]
	fn concurrently_registered_handlers_on_one_variant_are_all_indexed() {
		const ROUNDS: usize = 1000;

		let variant = VariantRef {
			sumtype_id: SumTypeId(1),
			variant_tag: 0,
		};

		for round in 0..ROUNDS {
			let cache = CatalogCache::new();
			let barrier = Arc::new(Barrier::new(2));

			// Two CREATE HANDLER transactions on the same event variant, in different namespaces.
			// Their read and write sets are disjoint, so the oracle does not make them conflict and
			// both commit; the cache is then updated from the post-commit interceptor, which nothing
			// serialises.
			let threads: Vec<_> = [1u64, 2u64]
				.into_iter()
				.map(|id| {
					let cache = cache.clone();
					let barrier = Arc::clone(&barrier);
					thread::spawn(move || {
						barrier.wait();
						cache.set_handler(
							HandlerId(id),
							CommitVersion(1),
							Some(handler(id, id, variant)),
						);
					})
				})
				.collect();

			for thread in threads {
				thread.join().unwrap();
			}

			let indexed = cache.list_handlers_for_variant_at(variant, CommitVersion(1));
			assert_eq!(
				indexed.len(),
				2,
				"round {round}: two handlers were registered on the same event variant but {} is \
				 indexed. Each writer read the variant's id list, appended its own id to a clone, and \
				 wrote the whole list back with no lock spanning the read and the write, so the slower \
				 one erased the other. list_handlers_for_variant reads only this index and never falls \
				 back to the store, so the erased handler silently stops firing - and nothing rebuilds \
				 the index, not even a restart",
				indexed.len()
			);
		}
	}
}
