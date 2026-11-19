// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, thread};

use reifydb_core::{
	commit::position::FlowCaughtUpPosition,
	common::CommitVersion,
	interface::{
		catalog::flow::FlowId,
		flow::{FlowPositionRow, FlowPositionSampler, FlowTickSampler},
		subscription::{SubscriptionPositionRow, SubscriptionPositionSampler},
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_value::{error::ValueError, value::duration::Duration};

use crate::Database;

pub struct Progress<'a> {
	db: &'a Database,
}

impl<'a> Progress<'a> {
	pub(crate) fn new(db: &'a Database) -> Self {
		Self {
			db,
		}
	}

	pub fn tx(&self) -> TxProgress<'a> {
		TxProgress {
			db: self.db,
		}
	}

	pub fn reader(&self) -> ReaderProgress<'a> {
		ReaderProgress {
			db: self.db,
			clock: Clock::Real,
		}
	}

	pub fn flow(&self) -> Option<FlowPositions<'a>> {
		let source = self.db.engine().ioc().resolve::<FlowPositionSampler>().ok()?;
		let ticks = self.db.engine().ioc().resolve::<FlowTickSampler>().ok()?;
		Some(FlowPositions {
			source,
			ticks,
			_marker: PhantomData,
		})
	}

	pub fn subscription(&self) -> Option<SubscriptionPositions<'a>> {
		let source = self.db.engine().ioc().resolve::<SubscriptionPositionSampler>().ok()?;
		Some(SubscriptionPositions {
			source,
			_marker: PhantomData,
		})
	}

	/// The only fallible read is `tx().current()`.
	pub fn snapshot(&self) -> Result<ProgressSnapshot, ValueError> {
		let tx = self.tx();
		let reader = self.reader();
		Ok(ProgressSnapshot {
			tx: TxSnapshot {
				current: tx.current()?,
				done_until: tx.done_until(),
			},
			reader: ReaderSnapshot {
				released: reader.released(),
				subscription: reader.subscription(),
				flow_caught_up: reader.flow_caught_up(),
			},
			flow: self.flow().map(|f| f.all()),
		})
	}
}

pub struct TxProgress<'a> {
	db: &'a Database,
}

impl TxProgress<'_> {
	/// Highest committed version on the engine. Advances on every successful commit.
	pub fn current(&self) -> Result<CommitVersion, ValueError> {
		self.db.engine().current_version()
	}

	/// Largest version V such that every commit `<= V` has finished. Safe
	/// boundary for commit readers and snapshot reads.
	pub fn done_until(&self) -> CommitVersion {
		self.db.engine().done_until()
	}
}

pub struct ReaderProgress<'a> {
	db: &'a Database,
	clock: Clock,
}

impl ReaderProgress<'_> {
	/// Highest commit version the commit queue has released in order. Advances even for commits
	/// whose deltas are all excluded by `is_commit_excluded`, so it is the done-until mark every reader
	/// is measured against.
	pub fn released(&self) -> CommitVersion {
		self.db.engine().commit_released()
	}

	/// How far the subscription reader has read the commit queue.
	pub fn subscription(&self) -> CommitVersion {
		self.db.engine().reader_position()
	}

	/// The version up to which every deferred flow has materialized its output, for chains of any
	/// depth: a hop's cursor passing a version is not enough, every flow must also have consumed
	/// the output any flow produced from it. `0` when no flow subsystem is running.
	pub fn flow_caught_up(&self) -> CommitVersion {
		self.db.engine()
			.ioc()
			.try_resolve::<FlowCaughtUpPosition>()
			.map(|w| w.get())
			.unwrap_or(CommitVersion(0))
	}

	/// Waits for the subscription reader to reach `version`, returning whether it got there.
	///
	/// `timeout` is wall-clock, deliberately not the database clock: a caller that builds with
	/// `RuntimeConfig::seeded(..)` gets a stopped `MockClock`, and a deadline derived from it can
	/// never be reached, turning a position that never arrives into a permanent hang instead of
	/// a bounded `false`.
	pub fn wait_for_subscription(&self, version: CommitVersion, timeout: Duration) -> bool {
		if self.subscription() >= version {
			return true;
		}
		let deadline = self.clock.instant() + timeout.to_std();
		loop {
			if self.subscription() >= version {
				return true;
			}
			if self.clock.instant() >= deadline {
				return self.subscription() >= version;
			}
			thread::sleep(Duration::from_milliseconds(2).unwrap().to_std());
		}
	}

	/// Waits for every deferred flow to have materialized output covering `version`, returning
	/// whether they got there. Wall-clock `timeout`, for the reason given on [`Self::wait_for_subscription`].
	pub fn wait_for_flow_caught_up(&self, version: CommitVersion, timeout: Duration) -> Result<bool, ValueError> {
		let caught_up = self.db.engine().ioc().try_resolve::<FlowCaughtUpPosition>();
		let deadline = self.clock.instant() + timeout.to_std();
		loop {
			if let Some(caught_up) = &caught_up {
				caught_up.ensure_healthy()?;
			}
			if self.flow_caught_up() >= version {
				return Ok(true);
			}
			if self.clock.instant() >= deadline {
				return Ok(self.flow_caught_up() >= version);
			}
			thread::sleep(Duration::from_milliseconds(2).unwrap().to_std());
		}
	}
}

pub struct FlowPositions<'a> {
	source: FlowPositionSampler,
	ticks: FlowTickSampler,
	_marker: PhantomData<&'a Database>,
}

impl FlowPositions<'_> {
	pub fn all(&self) -> Vec<FlowPositionRow> {
		self.source.all()
	}

	pub fn ticks(&self) -> Vec<(FlowId, u64)> {
		self.ticks.all()
	}
}

pub struct SubscriptionPositions<'a> {
	source: SubscriptionPositionSampler,
	_marker: PhantomData<&'a Database>,
}

impl SubscriptionPositions<'_> {
	pub fn all(&self) -> Vec<SubscriptionPositionRow> {
		self.source.all()
	}
}

#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
	pub tx: TxSnapshot,
	pub reader: ReaderSnapshot,
	pub flow: Option<Vec<FlowPositionRow>>,
}

#[derive(Debug, Clone, Copy)]
pub struct TxSnapshot {
	pub current: CommitVersion,
	pub done_until: CommitVersion,
}

#[derive(Debug, Clone, Copy)]
pub struct ReaderSnapshot {
	pub released: CommitVersion,
	pub subscription: CommitVersion,
	pub flow_caught_up: CommitVersion,
}
