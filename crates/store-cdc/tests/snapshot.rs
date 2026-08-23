// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_testing::testscript::runner::run_path;
use test_each_file::test_each_path;

mod common;
use common::{Fixture, runner::Runner};

test_each_path! { in "crates/store-cdc/tests/scripts/cdc" as cdc => test_every_tier_combination }

fn test_every_tier_combination(path: &Path) {
	// every combination replays the same script into the same goldenfile, so a tier that answers differently cannot
	// pass
	let combinations: [(&str, fn() -> Fixture); 5] = [
		("memory", common::memory),
		("memory_cached", common::memory_cached),
		("sqlite", common::sqlite),
		("sqlite_cached", common::sqlite_cached),
		("sqlite_starved_cache", common::sqlite_starved_cache),
	];
	for (name, fresh) in combinations {
		let fixture = fresh();
		let mut runner = Runner::new(fixture.store.clone(), fixture.persistent.clone());
		run_path(&mut runner, path).unwrap_or_else(|e| panic!("{name}: {e}"));
	}
}
