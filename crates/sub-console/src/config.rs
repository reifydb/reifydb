// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::PathBuf;

#[derive(Default)]
pub struct ConsoleConfigurator {
	address: Option<String>,

	token: Option<String>,

	fingerprint_path: Option<PathBuf>,
}

impl ConsoleConfigurator {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn address(mut self, address: impl Into<String>) -> Self {
		self.address = Some(address.into());
		self
	}

	pub fn token(mut self, token: impl Into<String>) -> Self {
		self.token = Some(token.into());
		self
	}

	pub fn fingerprint_path(mut self, path: impl Into<PathBuf>) -> Self {
		self.fingerprint_path = Some(path.into());
		self
	}

	pub(crate) fn configure(self) -> ConsoleConfig {
		ConsoleConfig {
			address: self.address,
			token: self.token,
			fingerprint_path: self.fingerprint_path,
		}
	}
}

#[derive(Clone, Debug)]
pub struct ConsoleConfig {
	pub address: Option<String>,

	pub token: Option<String>,

	pub fingerprint_path: Option<PathBuf>,
}
