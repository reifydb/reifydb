// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_store_single::tier::commit::buffer::SingleCommitBufferTier;
use reifydb_testing::testscript::runner::run_path;
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-single/tests/scripts/single" as store_single_memory => test_memory }

fn test_memory(path: &Path) {
	let storage = SingleCommitBufferTier::memory();
	run_path(&mut Runner::new(storage), path).expect("test failed")
}
