// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::Result;

use crate::common::CommitVersion;

pub trait EvictionWatermark: Send + Sync + 'static {
	fn watermark(&self) -> CommitVersion;
}

pub trait QueryWatermark: Send + Sync + 'static {
	fn effective_gc_cutoff(&self) -> CommitVersion;
}

pub trait ConsumerPositions: Send + Sync + 'static {
	fn min_position(&self) -> Option<CommitVersion>;
}

pub trait CheckpointFloor: Send + Sync + 'static {
	fn floor(&self) -> Result<Option<CommitVersion>>;
}
