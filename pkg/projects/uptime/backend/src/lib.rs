// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{env::var_os, path::PathBuf};

pub fn migration_path() -> PathBuf {
	var_os("MIGRATION_PATH")
		.map(PathBuf::from)
		.unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migration")))
}
