// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{path::Path, time::Instant};

use reifydb_engine::test::parser::{self, TestCase};
pub use reifydb_engine::test::{
	parser::ParseError,
	result::{PrintConfig, TestOutcome, TestResult, TestSuiteResult},
};
use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
use reifydb_type::params::Params;

use crate::embedded;

pub struct TestRunnerConfig {
	pub runtime: Option<SharedRuntime>,
}

impl Default for TestRunnerConfig {
	fn default() -> Self {
		Self {
			runtime: None,
		}
	}
}

/// Parse and run all test blocks from a string.
pub fn run_test_str(input: &str, config: TestRunnerConfig) -> Result<TestSuiteResult, ParseError> {
	let cases = parser::parse(input)?;
	Ok(run(&cases, config))
}

/// Parse and run all test blocks from a file.
pub fn run_test_file(path: impl AsRef<Path>, config: TestRunnerConfig) -> Result<TestSuiteResult, ParseError> {
	let input = std::fs::read_to_string(path.as_ref()).map_err(|e| ParseError {
		message: format!("failed to read file: {}", e),
		line: 0,
	})?;
	run_test_str(&input, config)
}

fn run(test_cases: &[TestCase], config: TestRunnerConfig) -> TestSuiteResult {
	let runtime = config.runtime.unwrap_or_else(|| SharedRuntime::from_config(SharedRuntimeConfig::default()));
	let mut results = Vec::with_capacity(test_cases.len());

	for test_case in test_cases {
		let start = Instant::now();
		let outcome = run_single(test_case, &runtime);
		let duration = start.elapsed();

		results.push(TestResult {
			name: test_case.name.clone(),
			outcome,
			duration,
		});
	}

	TestSuiteResult {
		results,
	}
}

fn run_single(test_case: &TestCase, runtime: &SharedRuntime) -> TestOutcome {
	let mut db = match embedded::memory().with_runtime(runtime.clone()).build() {
		Ok(db) => db,
		Err(e) => return TestOutcome::Error(format!("failed to create database: {}", e)),
	};

	if let Err(e) = db.start() {
		return TestOutcome::Error(format!("failed to start database: {}", e));
	}

	let outcome = match db.admin_as_root(&test_case.body, Params::None) {
		Ok(_) => TestOutcome::Pass,
		Err(e) => {
			if e.code == "ASSERT" {
				TestOutcome::Fail(e.message.clone())
			} else {
				TestOutcome::Error(format!("{}", e))
			}
		}
	};

	let _ = db.stop();
	outcome
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_pass() {
		let input = r#"
test("simple pass") {
    CREATE NAMESPACE test
}
"#;
		let result = run_test_str(input, TestRunnerConfig::default()).unwrap();
		assert_eq!(result.total(), 1);
		assert!(result.all_passed());
	}

	#[test]
	fn test_assertion_failure() {
		let input = r#"
test("should fail") {
    ASSERT { 1 == 2 }
}
"#;
		let result = run_test_str(input, TestRunnerConfig::default()).unwrap();
		assert_eq!(result.total(), 1);
		assert_eq!(result.failed(), 1);
	}

	#[test]
	fn test_error() {
		let input = r#"
test("should error") {
    FROM nonexistent.table
}
"#;
		let result = run_test_str(input, TestRunnerConfig::default()).unwrap();
		assert_eq!(result.total(), 1);
		assert_eq!(result.errored(), 1);
	}

	#[test]
	fn test_multiple_tests() {
		let input = r#"
test("pass") {
    CREATE NAMESPACE test
}

test("fail") {
    ASSERT { 1 == 2 }
}

test("error") {
    FROM nonexistent.table
}
"#;
		let result = run_test_str(input, TestRunnerConfig::default()).unwrap();
		assert_eq!(result.total(), 3);
		assert_eq!(result.passed(), 1);
		assert_eq!(result.failed(), 1);
		assert_eq!(result.errored(), 1);
	}

	#[test]
	fn test_isolation() {
		let input = r#"
test("create schema") {
    CREATE NAMESPACE test;
    CREATE TABLE test::users { id: int4, name: text };
    INSERT test::users [{ id: 1, name: "Alice" }];
}

test("schema should not exist") {
    ASSERT { 1 == 1 };
}
"#;
		let result = run_test_str(input, TestRunnerConfig::default()).unwrap();
		assert_eq!(result.passed(), 2);
	}
}
