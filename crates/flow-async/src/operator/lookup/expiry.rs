// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{key::operator::state::GroupId, state::timer::TimerKind};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use super::store::take_read;
use crate::{
	operator::{
		host::HostContext,
		join::expiry::JoinExpiryIndex,
		state::seal::{ledger::FiredAt, rule::SealRule},
	},
	transaction::join_expiry::DueStart,
};

const LEFT_SIDE: u8 = 0;

const EXPIRY_BATCH: usize = 256;

#[derive(Debug, Default)]
pub(crate) struct LookupExpiry {
	retention: Option<Duration>,
	index: JoinExpiryIndex,
}

impl LookupExpiry {
	pub(crate) fn new(retention: Option<Duration>) -> Self {
		Self {
			retention: retention.filter(|span| !span.is_zero()),
			index: JoinExpiryIndex::default(),
		}
	}

	pub(crate) fn retention(&self) -> Option<Duration> {
		self.retention
	}

	pub(crate) fn timer_key() -> EncodedKey {
		EncodedKey::new(Vec::new())
	}

	pub(crate) fn move_rows(
		&mut self,
		host: &mut dyn HostContext,
		cleared: &[RowNumber],
		armed: &[(RowNumber, DateTime)],
	) -> Result<()> {
		let Some(retention) = self.retention else {
			return Ok(());
		};
		if cleared.is_empty() && armed.is_empty() {
			return Ok(());
		}
		let rule = SealRule::of(retention);
		for row in cleared {
			if let Some(at) = host.join_expiry_clear(GroupId::ROOT, LEFT_SIDE, *row)? {
				self.index.cleared(at);
			}
		}
		for (row, at) in armed {
			let sealed = rule.seal_instant(*at).at();
			host.join_expiry_arm(GroupId::ROOT, LEFT_SIDE, *row, sealed)?;
			self.index.armed(sealed);
		}
		self.resync_timer(host)
	}

	pub(crate) fn free_due(&mut self, host: &mut dyn HostContext, fired: FiredAt) -> Result<()> {
		if self.retention.is_none() {
			return Ok(());
		}
		let mut start = match self.index.min(host)? {
			Some(floor) => DueStart::Floor(floor),
			None => DueStart::Bottom,
		};
		let earliest = loop {
			let page = host.join_due_page(fired.at(), EXPIRY_BATCH, &start)?;
			if page.due.is_empty() {
				break page.next;
			}
			for entry in &page.due {
				take_read(host, entry.row_number)?;
				host.join_expiry_free(entry)?;
			}
			if !page.more {
				break page.next;
			}
			start = match page.resume {
				Some(cursor) => DueStart::After(cursor),
				None => break page.next,
			};
		};
		self.index.settle(earliest);
		self.resync_timer(host)
	}

	fn resync_timer(&mut self, host: &mut dyn HostContext) -> Result<()> {
		match self.index.min(host)? {
			Some(at) => host.arm_timer(at, TimerKind::Maintenance, &Self::timer_key()),
			None => host.disarm_timer_by_key(TimerKind::Maintenance, &Self::timer_key()),
		}
	}
}
