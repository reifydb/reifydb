// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::Path;

use reifydb_engine::test_harness::TestEngine;
use reifydb_testing::testscript::runner::run_path;
use test_each_file::test_each_path;

mod common;

test_each_path! { in "crates/cdc/tests/scripts/cdc" as snapshot_memory => run_memory }

fn run_memory(path: &Path) {
	let engine = TestEngine::new();
	let cdc_store = engine.inner().cdc_store();
	let mock_clock = engine.mock_clock();
	let mut runner = common::Runner::new(engine.inner().clone(), cdc_store, mock_clock);
	run_path(&mut runner, path).expect("testscript failed")
}
