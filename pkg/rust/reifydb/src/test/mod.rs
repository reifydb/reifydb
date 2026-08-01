// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs::read_to_string, path::Path};

use reifydb_value::{params::Params, value::Value};

#[cfg(feature = "sub_flow")]
use crate::WithSubsystem;
use crate::embedded;

/// Runs the file content as-is via `admin_as_root` against a fresh in-memory database; a statement
/// error, or `RUN TESTS` reporting failures, comes back as `Err`.
pub fn run_test_file(path: impl AsRef<Path>) -> Result<(), String> {
	let content = read_to_string(path.as_ref()).map_err(|e| format!("failed to read file: {}", e))?;
	run_test_str(&content)
}

/// Run RQL test content against a fresh in-memory database.
pub fn run_test_str(content: &str) -> Result<(), String> {
	let builder = embedded::memory();
	#[cfg(feature = "sub_flow")]
	let builder = builder.with_flow(|flow| flow);
	let mut db = builder.build().map_err(|e| format!("failed to create database: {}", e))?;

	let result = db.admin_as_root(content, Params::None);

	let _ = db.stop();

	match result {
		Ok(frames) => {
			if let Some(frame) = frames.last() {
				let outcome_idx = frame.columns.iter().position(|c| c.name == "outcome");
				let name_idx = frame.columns.iter().position(|c| c.name == "name");
				let ns_idx = frame.columns.iter().position(|c| c.name == "namespace");
				let msg_idx = frame.columns.iter().position(|c| c.name == "message");

				if let Some(oi) = outcome_idx {
					let outcome_col = &frame.columns[oi];
					let row_count = outcome_col.data.len();
					let mut failures = Vec::new();

					for i in 0..row_count {
						let outcome = outcome_col.data.get_value(i);
						if matches!(&outcome, Value::Utf8(s) if s == "fail" || s == "error") {
							let prefix = match &outcome {
								Value::Utf8(s) if s == "fail" => "FAIL",
								_ => "ERROR",
							};
							let ns = ns_idx
								.map(|idx| frame.columns[idx].data.as_string(i))
								.unwrap_or_default();
							let name = name_idx
								.map(|idx| frame.columns[idx].data.as_string(i))
								.unwrap_or_default();
							let msg = msg_idx
								.map(|idx| frame.columns[idx].data.as_string(i))
								.unwrap_or_default();
							failures.push(format!("{} {}::{}: {}", prefix, ns, name, msg));
						}
					}

					if !failures.is_empty() {
						let failed = failures.len();
						let summary = failures.join("\n  ");
						return Err(format!(
							"{} of {} test(s) failed:\n  {}",
							failed, row_count, summary
						));
					}
				}
			}
			Ok(())
		}
		Err(e) => Err(format!("{}", e)),
	}
}
