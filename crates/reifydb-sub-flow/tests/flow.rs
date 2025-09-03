// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[path = "runner.rs"]
mod runner;

use std::path::Path;

use reifydb_testing::testscript;
use runner::FlowTestRunner;

pub fn test_flow(path: &Path) {
	testscript::run_path(&mut FlowTestRunner::new(), path)
		.expect("test failed")
}
