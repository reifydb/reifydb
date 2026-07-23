// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Default)]
pub struct GcMetrics {
	pub shapes_scanned: u64,
	pub versions_dropped: u64,
}
