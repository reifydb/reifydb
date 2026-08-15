// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_store_operator::store::OperatorStore;
use reifydb_testing::testscript::runner::run_path;
use test_each_file::test_each_path;

mod common;
use common::Runner;

test_each_path! { in "crates/store-operator/tests/scripts/state" as operator_memory_state => test_memory }
test_each_path! { in "crates/store-operator/tests/scripts/checkpoint" as operator_memory_checkpoint => test_memory }
test_each_path! { in "crates/store-operator/tests/scripts/anchor" as operator_memory_anchor => test_memory }

fn test_memory(path: &Path) {
	// these scripts share one goldenfile with the sqlite tiers, so any divergence must fail here
	run_path(&mut Runner::from_store_no_auto_flush(OperatorStore::testing_memory()), path).expect("test failed")
}
