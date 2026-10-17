// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::common::CommitVersion;
use reifydb_value::Result;

pub trait VersionProvider: Send + Sync + Clone {
	fn next(&self) -> Result<CommitVersion>;
	fn reserve(&self) -> Result<CommitVersion>;
	fn publish(&self, version: CommitVersion);
	fn current(&self) -> Result<CommitVersion>;

	fn advance_to(&self, _version: CommitVersion) {}
}

#[derive(Clone)]
pub struct StandardVersionProvider {
	next_version: Arc<AtomicU64>,
}

impl StandardVersionProvider {
	pub fn new(last: CommitVersion) -> Self {
		Self {
			next_version: Arc::new(AtomicU64::new(last.0)),
		}
	}
}

impl VersionProvider for StandardVersionProvider {
	fn next(&self) -> Result<CommitVersion> {
		Ok(CommitVersion(self.next_version.fetch_add(1, Ordering::SeqCst) + 1))
	}

	fn reserve(&self) -> Result<CommitVersion> {
		Ok(CommitVersion(self.next_version.load(Ordering::SeqCst) + 1))
	}

	fn publish(&self, version: CommitVersion) {
		self.next_version.fetch_max(version.0, Ordering::SeqCst);
	}

	fn current(&self) -> Result<CommitVersion> {
		Ok(CommitVersion(self.next_version.load(Ordering::SeqCst)))
	}

	fn advance_to(&self, version: CommitVersion) {
		self.next_version.fetch_max(version.0, Ordering::SeqCst);
	}
}

#[cfg(test)]
pub mod tests {
	use std::{sync::Arc, thread};

	use super::*;

	#[test]
	fn test_new_version_provider() {
		// Must start at the last version seen anywhere, otherwise a restart reissues or skips versions.
		let provider = StandardVersionProvider::new(CommitVersion(0));
		assert_eq!(provider.current().unwrap(), 0);

		let recovered = StandardVersionProvider::new(CommitVersion(500));
		assert_eq!(recovered.current().unwrap(), 500);
		assert_eq!(recovered.next().unwrap(), 501);
	}

	#[test]
	fn test_next_version_sequential() {
		let provider = StandardVersionProvider::new(CommitVersion(0));

		assert_eq!(provider.next().unwrap(), 1);
		assert_eq!(provider.current().unwrap(), 1);

		assert_eq!(provider.next().unwrap(), 2);
		assert_eq!(provider.current().unwrap(), 2);

		assert_eq!(provider.next().unwrap(), 3);
		assert_eq!(provider.current().unwrap(), 3);
	}

	#[test]
	fn test_concurrent_version_allocation() {
		let provider = Arc::new(StandardVersionProvider::new(CommitVersion(0)));

		let mut handles = vec![];

		for _ in 0..10 {
			let provider_clone = Arc::clone(&provider);
			let handle = thread::spawn(move || {
				let mut versions = vec![];
				for _ in 0..100 {
					versions.push(provider_clone.next().unwrap());
				}
				versions
			});
			handles.push(handle);
		}

		let mut all_versions = vec![];
		for handle in handles {
			let mut versions = handle.join().unwrap();
			all_versions.append(&mut versions);
		}

		all_versions.sort();

		// A duplicate version would make two transactions share one commit point.
		for i in 1..all_versions.len() {
			assert_ne!(
				all_versions[i - 1],
				all_versions[i],
				"Duplicate version found: {}",
				all_versions[i]
			);
		}

		// 10 threads times 100 versions, with no gaps.
		assert_eq!(all_versions.len(), 1000);

		assert_eq!(all_versions[0], 1);
		assert_eq!(all_versions[999], 1000);
	}

	#[test]
	fn a_reserved_version_stays_invisible_until_published() {
		// The oracle registers a reserved version on both watermarks before publishing it;
		// if reserve bumped the public counter, a concurrent begin could snapshot at a
		// version no watermark knows about, and the commit frontier could pass it before
		// registration - the torn-snapshot / lost-CDC window this API exists to close.
		let provider = StandardVersionProvider::new(CommitVersion(0));

		let reserved = provider.reserve().unwrap();
		assert_eq!(reserved, 1);
		assert_eq!(provider.current().unwrap(), 0, "reserve must not move the public version");

		let reserved_again = provider.reserve().unwrap();
		assert_eq!(reserved_again, 1, "an unpublished reservation must not consume the version");

		provider.publish(reserved);
		assert_eq!(provider.current().unwrap(), 1);
		assert_eq!(provider.next().unwrap(), 2);
	}
}
