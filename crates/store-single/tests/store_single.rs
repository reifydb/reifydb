// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_store_single::{buffer::tier::SingleBufferTier, config::PersistentConfig};
use reifydb_testing::{tempdir::temp_dir, testscript::runner::run_path};
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-single/tests/scripts/single" as store_single_memory => test_memory }
test_each_path! { in "crates/store-single/tests/scripts/single" as store_single_sqlite_unbuffered => test_sqlite_unbuffered }

fn test_memory(path: &Path) {
	let storage = SingleBufferTier::memory();
	run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite_unbuffered(path: &Path) {
	temp_dir(|_db_path| {
		let (config, _guard) = PersistentConfig::sqlite_in_memory();
		run_path(&mut Runner::sqlite_unbuffered(config), path)
	})
	.expect("test failed")
}
