// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::Path;

use reifydb::test::{PrintConfig, TestRunnerConfig, run_test_file};
use test_each_file::test_each_path;

test_each_path! { in "pkg/rust/reifydb/tests/rql" as rql => test_rql_file }

fn test_rql_file(path: &Path) {
	let result = run_test_file(path, TestRunnerConfig::default()).unwrap();
	result.print_summary(&PrintConfig::default());
	if !result.all_passed() {
		panic!("{} test(s) did not pass", result.failed() + result.errored());
	}
}
