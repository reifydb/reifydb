// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_build::{emit_export_dynamic_bins, emit_target_cfg};

fn main() {
	emit_target_cfg();
	emit_export_dynamic_bins();
}
