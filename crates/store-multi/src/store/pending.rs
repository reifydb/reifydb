// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::store::{EntryKind, classify_key},
};
use reifydb_runtime::sync::rwlock::RwLock;
use tracing::{error, instrument};

use crate::tier::{persistent::MultiPersistentTier, read::MultiReadBufferTier};

#[derive(Clone, Default)]
pub struct PendingDrops {
	inner: Arc<PendingDropsInner>,
}

#[derive(Default)]
struct PendingDropsInner {
	map: RwLock<HashMap<EncodedKey, CommitVersion>>,
	len: AtomicUsize,
}

impl PendingDrops {
	pub fn record(&self, key: EncodedKey, version: CommitVersion) {
		let mut map = self.inner.map.write();
		match map.get_mut(&key) {
			Some(existing) => {
				if *existing < version {
					*existing = version;
				}
			}
			None => {
				map.insert(key, version);
				self.inner.len.fetch_add(1, Ordering::Relaxed);
			}
		}
	}

	pub fn masks(&self, key: &EncodedKey, row_version: CommitVersion, read: CommitVersion) -> bool {
		if self.is_empty() {
			return false;
		}
		let map = self.inner.map.read();
		match map.get(key) {
			Some(dropped) => row_version < *dropped && *dropped <= read,
			None => false,
		}
	}

	pub fn settle(&self, key: &EncodedKey, through: CommitVersion) {
		let mut map = self.inner.map.write();
		if map.get(key).is_some_and(|dropped| *dropped <= through) && map.remove(key).is_some() {
			self.inner.len.fetch_sub(1, Ordering::Relaxed);
		}
	}

	pub fn is_empty(&self) -> bool {
		self.inner.len.load(Ordering::Relaxed) == 0
	}

	#[instrument(name = "drop::purge_pending", level = "debug", skip_all)]
	pub fn purge(&self, persistent: Option<&MultiPersistentTier>, read: Option<&MultiReadBufferTier>) {
		if self.is_empty() {
			return;
		}
		let mut by_kind: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		{
			let map = self.inner.map.read();
			for (key, version) in map.iter() {
				by_kind.entry(classify_key(key)).or_default().push((key.clone(), *version));
			}
		}
		for (kind, keys) in by_kind {
			if let Some(persistent) = persistent
				&& let Err(e) = persistent.delete_keys_through(kind, &keys)
			{
				error!(?kind, error = %e, "Failed to purge dropped rows, keeping them pending");
				continue;
			}
			for (key, version) in &keys {
				if let Some(read) = read {
					read.remove_dropped_through(key, *version);
				}
				self.settle(key, *version);
			}
		}
	}
}
