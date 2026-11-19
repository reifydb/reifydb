// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::ValueError, value::datetime::DateTime};

use crate::operator::host::HostContext;

#[derive(Debug, Default)]
pub(crate) struct JoinForgetIndex {
	earliest: Option<Option<DateTime>>,
}

impl JoinForgetIndex {
	pub(crate) fn armed(&mut self, at: DateTime) {
		if let Some(earliest) = self.earliest.as_mut() {
			*earliest = Some(earliest.map_or(at, |seen| seen.min(at)));
		}
	}

	pub(crate) fn cleared(&mut self, at: DateTime) {
		if self.earliest == Some(Some(at)) {
			self.earliest = None;
		}
	}

	pub(crate) fn min(&mut self, host: &mut dyn HostContext) -> Result<Option<DateTime>, ValueError> {
		if let Some(earliest) = self.earliest {
			return Ok(earliest);
		}
		let earliest = host.join_forget_min()?;
		self.earliest = Some(earliest);
		Ok(earliest)
	}

	pub(crate) fn settle(&mut self, earliest: Option<DateTime>) {
		self.earliest = Some(earliest);
	}
}
