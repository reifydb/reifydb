// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
	fragment::Fragment,
};

pub trait Shutdown {
	fn shutdown(&self) -> Result<()>;
}

pub fn shutdown_failed(subsystem: impl fmt::Display, reason: impl fmt::Display) -> Diagnostic {
	Diagnostic {
		code: "SUB_004".to_string(),
		rql: None,
		message: format!("{} subsystem shutdown failed: {}", subsystem, reason),
		column: None,
		fragment: Fragment::None,
		label: None,
		help: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub struct ShutdownErrors {
	owner: &'static str,
	failed: Vec<String>,
}

impl ShutdownErrors {
	pub fn new(owner: &'static str) -> Self {
		Self {
			owner,
			failed: Vec::new(),
		}
	}

	pub fn step(&mut self, part: &'static str, result: Result<()>) {
		let Err(err) = result else {
			return;
		};
		let diagnostic = err.diagnostic();
		if diagnostic.code == "SUB_004" {
			self.failed.extend(diagnostic.notes.into_iter().map(|note| format!("{part}/{note}")));
		} else {
			self.failed.push(format!("{part}: {}", diagnostic.message));
		}
	}

	pub fn fail(&mut self, part: &'static str, reason: impl fmt::Display) {
		self.failed.push(format!("{part}: {reason}"));
	}

	pub fn finish(self) -> Result<()> {
		if self.failed.is_empty() {
			return Ok(());
		}
		let reason = format!("{} parts failed", self.failed.len());
		Err(Error(Box::new(Diagnostic {
			notes: self.failed,
			..shutdown_failed(self.owner, reason)
		})))
	}
}
