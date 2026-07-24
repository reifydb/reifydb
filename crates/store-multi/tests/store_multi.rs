// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_store_multi::tier::commit::buffer::MultiCommitBufferTier;
use reifydb_testing::testscript::runner::run_path;
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-multi/tests/scripts/multi" as store_multi_memory => test_memory }

test_each_path! { in "crates/store-multi/tests/scripts/historical" as store_multi_historical_memory => test_memory }

fn test_memory(path: &Path) {
	let storage = MultiCommitBufferTier::memory();
	run_path(&mut Runner::new(storage), path).expect("test failed")
}
