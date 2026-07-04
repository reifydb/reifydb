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
use reifydb_core::common::CommitVersion;
use reifydb_runtime::sync::rwlock::RwLock;

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
}
