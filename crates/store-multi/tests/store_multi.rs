// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::Path;

use reifydb_store_multi::hot::storage::HotStorage;
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-multi/tests/scripts/multi" as store_multi_memory => test_memory }
test_each_path! { in "crates/store-multi/tests/scripts/multi" as store_multi_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let storage = HotStorage::memory();
	run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let storage = HotStorage::sqlite_in_memory();
		run_path(&mut Runner::new(storage), path)
	})
	.expect("test failed")
}
