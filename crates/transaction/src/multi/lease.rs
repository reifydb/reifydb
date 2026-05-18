// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::common::CommitVersion;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_type::Result;

use crate::error::TransactionError;

#[derive(Debug)]
pub struct VersionLeases {
	inner: Mutex<BTreeMap<CommitVersion, u32>>,
}

impl VersionLeases {
	pub fn new() -> Arc<Self> {
		Arc::new(Self {
			inner: Mutex::new(BTreeMap::new()),
		})
	}

	pub fn try_acquire(self: &Arc<Self>, version: CommitVersion, qdu: CommitVersion) -> Result<VersionLeaseGuard> {
		let mut leases = self.inner.lock();
		Self::insert_locked(self, &mut leases, version, qdu)
	}

	pub fn try_acquire_with<F, T>(self: &Arc<Self>, f: F) -> Result<(VersionLeaseGuard, T)>
	where
		F: FnOnce() -> Result<(CommitVersion, CommitVersion, T)>,
	{
		let mut leases = self.inner.lock();
		let (version, qdu, extra) = f()?;
		let guard = Self::insert_locked(self, &mut leases, version, qdu)?;
		Ok((guard, extra))
	}

	pub fn min_active(&self) -> Option<CommitVersion> {
		self.inner.lock().keys().next().copied()
	}

	fn insert_locked(
		self_arc: &Arc<Self>,
		leases: &mut BTreeMap<CommitVersion, u32>,
		version: CommitVersion,
		qdu: CommitVersion,
	) -> Result<VersionLeaseGuard> {
		let cutoff = leases.keys().next().copied().unwrap_or(CommitVersion(u64::MAX)).min(qdu);
		if version < cutoff {
			return Err(TransactionError::SnapshotVersionEvicted {
				version,
				cutoff,
			}
			.into());
		}
		*leases.entry(version).or_insert(0) += 1;
		Ok(VersionLeaseGuard(Arc::new(VersionLeaseInner {
			leases: self_arc.clone(),
			version,
		})))
	}
}

#[derive(Debug)]
struct VersionLeaseInner {
	leases: Arc<VersionLeases>,
	version: CommitVersion,
}

impl Drop for VersionLeaseInner {
	fn drop(&mut self) {
		let mut map = self.leases.inner.lock();
		if let Some(count) = map.get_mut(&self.version) {
			*count -= 1;
			if *count == 0 {
				map.remove(&self.version);
			}
		}
	}
}

#[derive(Clone, Debug)]
pub struct VersionLeaseGuard(Arc<VersionLeaseInner>);

impl VersionLeaseGuard {
	pub fn version(&self) -> CommitVersion {
		self.0.version
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn try_acquire_succeeds_when_above_cutoff() {
		let leases = VersionLeases::new();
		let guard = leases.try_acquire(CommitVersion(10), CommitVersion(5)).expect("acquire");
		assert_eq!(guard.version(), CommitVersion(10));
		assert_eq!(leases.min_active(), Some(CommitVersion(10)));
	}

	#[test]
	fn try_acquire_succeeds_at_qdu() {
		let leases = VersionLeases::new();
		let guard = leases.try_acquire(CommitVersion(7), CommitVersion(7)).expect("acquire");
		assert_eq!(guard.version(), CommitVersion(7));
	}

	#[test]
	fn try_acquire_fails_below_cutoff() {
		let leases = VersionLeases::new();
		let err = leases.try_acquire(CommitVersion(3), CommitVersion(10)).unwrap_err();
		assert!(format!("{:?}", err).contains("TXN_012") || err.0.code == "TXN_012");
	}

	#[test]
	fn try_acquire_fails_below_existing_min_active() {
		let leases = VersionLeases::new();
		let _g = leases.try_acquire(CommitVersion(20), CommitVersion(5)).expect("acquire");
		let err = leases.try_acquire(CommitVersion(10), CommitVersion(25)).unwrap_err();
		assert_eq!(err.0.code, "TXN_012");
	}

	#[test]
	fn drop_releases_lease() {
		let leases = VersionLeases::new();
		{
			let _g = leases.try_acquire(CommitVersion(5), CommitVersion(0)).expect("acquire");
			assert_eq!(leases.min_active(), Some(CommitVersion(5)));
		}
		assert_eq!(leases.min_active(), None);
	}

	#[test]
	fn refcount_keeps_lease_alive_until_last_drop() {
		let leases = VersionLeases::new();
		let g1 = leases.try_acquire(CommitVersion(5), CommitVersion(0)).expect("acquire");
		let g2 = leases.try_acquire(CommitVersion(5), CommitVersion(0)).expect("acquire");
		drop(g1);
		assert_eq!(leases.min_active(), Some(CommitVersion(5)));
		drop(g2);
		assert_eq!(leases.min_active(), None);
	}

	#[test]
	fn clone_keeps_lease_alive_until_last_clone_drops() {
		let leases = VersionLeases::new();
		let g = leases.try_acquire(CommitVersion(5), CommitVersion(0)).expect("acquire");
		let g_clone = g.clone();
		drop(g);
		assert_eq!(leases.min_active(), Some(CommitVersion(5)));
		drop(g_clone);
		assert_eq!(leases.min_active(), None);
	}

	#[test]
	fn min_active_tracks_smallest_version() {
		let leases = VersionLeases::new();
		let _a = leases.try_acquire(CommitVersion(20), CommitVersion(0)).expect("acquire");
		let _b = leases.try_acquire(CommitVersion(15), CommitVersion(0)).expect("acquire");
		let _c = leases.try_acquire(CommitVersion(30), CommitVersion(0)).expect("acquire");
		assert_eq!(leases.min_active(), Some(CommitVersion(15)));
	}
}
