// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::path::Path;

use reifydb::test::run_test_file;
use test_each_file::test_each_path;

test_each_path! { in "pkg/rust/reifydb/tests/rql" as rql => test_rql_file }

fn test_rql_file(path: &Path) {
	if let Err(msg) = run_test_file(path) {
		panic!("{}", msg);
	}
}
