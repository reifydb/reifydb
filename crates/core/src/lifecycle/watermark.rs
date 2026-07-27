// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::CommitVersion;

pub trait EvictionWatermark: Send + Sync + 'static {
	fn watermark(&self) -> CommitVersion;
}

pub trait QueryWatermark: Send + Sync + 'static {
	fn effective_gc_cutoff(&self) -> CommitVersion;
}

pub trait ConsumerPositions: Send + Sync + 'static {
	/// The slowest live consumer position, or `None` when nothing is consuming.
	///
	/// `None` must mean "inert", never `CommitVersion(0)`: a zero would read as a consumer
	/// parked at the very beginning and would pin every reclaimer that consults this term.
	fn min_position(&self) -> Option<CommitVersion>;
}
