// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Version-epoch population. The `VersionEpoch` map itself lives in `reifydb-runtime`
//! (`reifydb_runtime::version_epoch::VersionEpoch`) so the flow engine can share it via
//! `RuntimeContext`. Two complementary feeders keep it current:
//!
//! - [`listener`] is the PRIMARY feed: it records `(commit wall-clock, commit version)` on every commit, so the map
//!   carries the exact instant each version was assigned. This is what the version-anchored TTL needs - a row's age is
//!   the wall-clock distance from its commit instant, and only per-commit recording captures the version of commits
//!   that share an instant (notably the flow-processing commits triggered by a source write).
//! - [`actor`] is a periodic BACKSTOP: it samples `(now, current version)` on an interval, so a commit event dropped
//!   under mailbox pressure is still eventually reflected. Same-instant samples collapse to the highest version, so the
//!   backstop never lowers a floor the listener already set.

pub mod actor;
pub mod listener;

use reifydb_core::common::CommitVersion;

/// Supplies the sampler with the current wall-clock time and the current commit version.
pub trait EpochSource: Send + Sync + 'static {
	fn now_nanos(&self) -> u64;

	fn current_version(&self) -> Option<CommitVersion>;
}
