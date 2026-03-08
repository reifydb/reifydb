// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{io::Write, time::Duration};

use reifydb_type::error::Error;

pub struct PrintConfig {
	pub color: bool,
}

impl Default for PrintConfig {
	fn default() -> Self {
		Self {
			color: true,
		}
	}
}

#[derive(Debug)]
pub enum TestOutcome {
	Pass,
	Fail(String),
	Error(String),
}

#[derive(Debug)]
pub struct TestResult {
	pub name: String,
	pub outcome: TestOutcome,
	pub duration: Duration,
}

#[derive(Debug)]
pub struct TestSuiteResult {
	pub results: Vec<TestResult>,
}

impl TestSuiteResult {
	pub fn passed(&self) -> usize {
		self.results.iter().filter(|r| matches!(r.outcome, TestOutcome::Pass)).count()
	}

	pub fn failed(&self) -> usize {
		self.results.iter().filter(|r| matches!(r.outcome, TestOutcome::Fail(_))).count()
	}

	pub fn errored(&self) -> usize {
		self.results.iter().filter(|r| matches!(r.outcome, TestOutcome::Error(_))).count()
	}

	pub fn total(&self) -> usize {
		self.results.len()
	}

	pub fn all_passed(&self) -> bool {
		self.results.iter().all(|r| matches!(r.outcome, TestOutcome::Pass))
	}

	pub fn print_summary(&self, config: &PrintConfig) {
		let (green, red, bold, reset) = if config.color {
			("\x1b[32m", "\x1b[31m", "\x1b[1m", "\x1b[0m")
		} else {
			("", "", "", "")
		};

		// Write directly to stderr to bypass cargo test output capture.
		// The eprint!/eprintln! macros go through Rust's OUTPUT_CAPTURE,
		// but writeln!(stderr(), ...) does not.
		let mut out = std::io::stderr();

		for result in &self.results {
			match &result.outcome {
				TestOutcome::Pass => {
					let _ = writeln!(
						out,
						"  {green}PASS{reset}  {} ({:?})",
						result.name, result.duration
					);
				}
				TestOutcome::Fail(msg) => {
					let _ = writeln!(
						out,
						"  {red}FAIL{reset}  {} ({:?})",
						result.name, result.duration
					);
					let _ = writeln!(out, "        {red}{}{reset}", msg);
				}
				TestOutcome::Error(msg) => {
					let _ = writeln!(
						out,
						"  {red}ERROR{reset} {} ({:?})",
						result.name, result.duration
					);
					let _ = writeln!(out, "        {red}{}{reset}", msg);
				}
			}
		}
		let _ = writeln!(out);
		let _ = writeln!(
			out,
			"{bold}{} passed, {} failed, {} errors, {} total{reset}",
			self.passed(),
			self.failed(),
			self.errored(),
			self.total()
		);
	}
}

pub fn classify_outcome(result: Result<(), &Error>) -> TestOutcome {
	match result {
		Ok(()) => TestOutcome::Pass,
		Err(e) if e.code == "ASSERT" => TestOutcome::Fail(e.message.clone()),
		Err(e) => TestOutcome::Error(format!("{}", e)),
	}
}
