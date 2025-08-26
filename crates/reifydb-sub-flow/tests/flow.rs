// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod runner;

use std::path::Path;

use reifydb_testing::testscript;
use runner::FlowTestRunner;
use test_each_file::test_each_path;

test_each_path! { in "crates/reifydb-sub-flow/tests/scripts/operators" as operators => test_flow }
test_each_path! { in "crates/reifydb-sub-flow/tests/scripts/smoke" as smoke=> test_flow }

fn test_flow(path: &Path) {
	testscript::run_path(&mut FlowTestRunner::new(), path)
		.expect("test failed")
}
