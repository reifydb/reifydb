// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{key::operator::state::GroupId, state::timer::TimerKind};
use reifydb_value::{
	error::ValueError,
	util::hash::Hash128,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use super::store::{ReadLedger, StoredRead};
use crate::{
	operator::{
		host::HostContext,
		join::{forget::JoinForgetIndex, state::JoinSide, store::Store},
		state::seal::{ledger::FiredAt, rule::SealRule},
	},
	transaction::join_forget::DueStart,
};

const LEFT_SIDE: u8 = 0;

const FORGET_BATCH: usize = 256;

#[derive(Debug, Default)]
pub(crate) struct LookupForget {
	lateness: Option<Duration>,
	index: JoinForgetIndex,
}

impl LookupForget {
	pub(crate) fn new(lateness: Option<Duration>) -> Self {
		Self {
			lateness,
			index: JoinForgetIndex::default(),
		}
	}

	pub(crate) fn lateness(&self) -> Option<Duration> {
		self.lateness
	}

	pub(crate) fn timer_key() -> EncodedKey {
		EncodedKey::new(Vec::new())
	}

	pub(crate) fn move_rows(
		&mut self,
		host: &mut dyn HostContext,
		cleared: &[RowNumber],
		armed: &[(RowNumber, DateTime)],
	) -> Result<(), ValueError> {
		let Some(lateness) = self.lateness else {
			return Ok(());
		};
		if cleared.is_empty() && armed.is_empty() {
			return Ok(());
		}
		let rule = SealRule::of(lateness);
		let cleared: Vec<(GroupId, RowNumber)> = cleared.iter().map(|row| (GroupId::ROOT, *row)).collect();
		for at in host.join_forget_clear(LEFT_SIDE, &cleared)?.into_iter().flatten() {
			self.index.cleared(at);
		}
		let sealed: Vec<(GroupId, RowNumber, DateTime)> =
			armed.iter().map(|(row, at)| (GroupId::ROOT, *row, rule.seal_instant(*at).at())).collect();
		host.join_forget_arm(LEFT_SIDE, &sealed)?;
		for &(.., at) in &sealed {
			self.index.armed(at);
		}
		self.resync_timer(host)
	}

	pub(crate) fn free_due(&mut self, host: &mut dyn HostContext, fired: FiredAt) -> Result<(), ValueError> {
		if self.lateness.is_none() {
			return Ok(());
		}
		let mut start = match self.index.min(host)? {
			Some(earliest) => DueStart::From(earliest),
			None => DueStart::Bottom,
		};
		let earliest = loop {
			let page = host.join_due_page(fired.at(), FORGET_BATCH, &start)?;
			if page.due.is_empty() {
				break page.next;
			}
			let mut ledger = ReadLedger::read(host, page.due.iter().map(|entry| entry.row_number))?;
			let left = Store::new(JoinSide::Left);
			for entry in &page.due {
				if let Some(StoredRead {
					key: Some(key),
					..
				}) = ledger.take(entry.row_number)?
				{
					left.remove_row_in(host, left.group_of(&Hash128(key.0)), &[entry.row_number])?;
				}
			}
			ledger.commit(host)?;
			host.join_forget_free(&page.due)?;
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

	fn resync_timer(&mut self, host: &mut dyn HostContext) -> Result<(), ValueError> {
		match self.index.min(host)? {
			Some(at) => host.arm_timer(at, TimerKind::ForgetPass, &Self::timer_key()),
			None => host.disarm_timer_by_key(TimerKind::ForgetPass, &Self::timer_key()),
		}
	}
}
