// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::error::Error;

#[derive(Debug)]
pub enum TestOutcome {
	Pass,
	Fail(String),
	Error(String),
}

pub fn classify_outcome(result: Result<(), &Error>) -> TestOutcome {
	match result {
		Ok(()) => TestOutcome::Pass,
		Err(e) if e.code == "ASSERT" => TestOutcome::Fail(e.message.clone()),
		Err(e) => TestOutcome::Error(format!("{}", e)),
	}
}
