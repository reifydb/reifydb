// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{Result, value::datetime::DateTime};

use crate::operator::host::HostContext;

#[derive(Debug, Default)]
pub(crate) struct JoinExpiryIndex {
	earliest: Option<Option<DateTime>>,
}

impl JoinExpiryIndex {
	pub(crate) fn armed(&mut self, at: DateTime) {
		if let Some(earliest) = self.earliest.as_mut() {
			*earliest = Some(earliest.map_or(at, |seen| seen.min(at)));
		}
	}

	pub(crate) fn invalidate(&mut self) {
		self.earliest = None;
	}

	pub(crate) fn min(&mut self, host: &mut dyn HostContext) -> Result<Option<DateTime>> {
		if let Some(earliest) = self.earliest {
			return Ok(earliest);
		}
		let earliest = host.join_expiry_min()?;
		self.earliest = Some(earliest);
		Ok(earliest)
	}
}
