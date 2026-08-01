// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, thread};

use reifydb_cdc::consume::watermark::FlowCaughtUpWatermark;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		flow::{FlowWatermarkRow, FlowWatermarkSampler},
		subscription::{SubscriptionWatermarkRow, SubscriptionWatermarkSampler},
	},
};
use reifydb_runtime::context::clock::Clock;
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::replica::watermark::ReplicaWatermark;
use reifydb_value::{Result, value::duration::Duration};

use crate::Database;

pub struct Watermarks<'a> {
	db: &'a Database,
}

impl<'a> Watermarks<'a> {
	pub(crate) fn new(db: &'a Database) -> Self {
		Self {
			db,
		}
	}

	pub fn tx(&self) -> TxWatermarks<'a> {
		TxWatermarks {
			db: self.db,
		}
	}

	pub fn cdc(&self) -> CdcWatermarks<'a> {
		CdcWatermarks {
			db: self.db,
			clock: Clock::Real,
		}
	}

	pub fn flow(&self) -> Option<FlowWatermarks<'a>> {
		let source = self.db.engine().ioc().resolve::<FlowWatermarkSampler>().ok()?;
		Some(FlowWatermarks {
			source,
			_marker: PhantomData,
		})
	}

	pub fn subscription(&self) -> Option<SubscriptionWatermarks<'a>> {
		let source = self.db.engine().ioc().resolve::<SubscriptionWatermarkSampler>().ok()?;
		Some(SubscriptionWatermarks {
			source,
			_marker: PhantomData,
		})
	}

	#[cfg(feature = "sub_replication")]
	pub fn replica(&self) -> Option<ReplicaWatermarks<'a>> {
		let watermark = self.db.engine().ioc().resolve::<ReplicaWatermark>().ok()?;
		Some(ReplicaWatermarks {
			watermark,
			_marker: PhantomData,
		})
	}

	/// The only fallible read is `tx().current()`.
	pub fn snapshot(&self) -> Result<WatermarkSnapshot> {
		let tx = self.tx();
		let cdc = self.cdc();
		Ok(WatermarkSnapshot {
			tx: TxSnapshot {
				current: tx.current()?,
				done_until: tx.done_until(),
			},
			cdc: CdcSnapshot {
				producer: cdc.producer(),
				max: cdc.max(),
				consumer: cdc.consumer(),
			},
			flow: self.flow().map(|f| f.all()),
			#[cfg(feature = "sub_replication")]
			replica: self.replica().map(|r| r.current()),
			#[cfg(not(feature = "sub_replication"))]
			replica: None,
		})
	}
}

pub struct TxWatermarks<'a> {
	db: &'a Database,
}

impl TxWatermarks<'_> {
	/// Highest committed version on the engine. Advances on every successful
	/// commit, regardless of CDC or replication.
	pub fn current(&self) -> Result<CommitVersion> {
		self.db.engine().current_version()
	}

	/// Largest version V such that every commit `<= V` has finished. Safe
	/// boundary for CDC consumers and snapshot reads.
	pub fn done_until(&self) -> CommitVersion {
		self.db.engine().done_until()
	}
}

pub struct CdcWatermarks<'a> {
	db: &'a Database,
	clock: Clock,
}

impl CdcWatermarks<'_> {
	/// Highest commit version processed by the CDC producer. Advances even for
	/// commits whose deltas are entirely filtered out by `should_exclude_from_cdc`,
	/// so it is the correct frontier for "producer is caught up to the engine".
	pub fn producer(&self) -> CommitVersion {
		self.db.engine().cdc_producer_watermark()
	}

	/// Largest version that has a row in the CDC store. Permanently lags by the commits whose
	/// deltas were entirely excluded from CDC (e.g. `ConfigStorage`-only ones); use `producer()`
	/// to ask whether the producer is caught up.
	pub fn max(&self) -> CommitVersion {
		self.db.engine().cdc_store().max_version().ok().flatten().unwrap_or(CommitVersion(0))
	}

	pub fn consumer(&self) -> CommitVersion {
		self.db.engine().cdc_consumer_watermark()
	}

	/// The version up to which every deferred flow has materialized its output, for chains of any
	/// depth: a hop's cursor passing a version is not enough, every flow must also have consumed
	/// the output any flow produced from it. `0` when no flow subsystem is running.
	pub fn flow_consumer(&self) -> CommitVersion {
		self.db.engine()
			.ioc()
			.try_resolve::<FlowCaughtUpWatermark>()
			.map(|w| w.get())
			.unwrap_or(CommitVersion(0))
	}

	/// Waits for the CDC consumer to reach `version`, returning whether it got there.
	///
	/// `timeout` is wall-clock, deliberately not the database clock: a caller that builds with
	/// `RuntimeConfig::seeded(..)` gets a frozen `MockClock`, and a deadline derived from it can
	/// never be reached, turning a watermark that never arrives into a permanent hang instead of
	/// a bounded `false`.
	pub fn wait_for_consumer(&self, version: CommitVersion, timeout: Duration) -> bool {
		if self.consumer() >= version {
			return true;
		}
		let deadline = self.clock.instant() + timeout.to_std();
		loop {
			if self.consumer() >= version {
				return true;
			}
			if self.clock.instant() >= deadline {
				return self.consumer() >= version;
			}
			thread::sleep(Duration::from_milliseconds(2).unwrap().to_std());
		}
	}

	/// Waits for every deferred flow to have materialized output covering `version`, returning
	/// whether they got there. Wall-clock `timeout`, for the reason given on [`Self::wait_for_consumer`].
	pub fn wait_for_flow_consumer(&self, version: CommitVersion, timeout: Duration) -> bool {
		if self.flow_consumer() >= version {
			return true;
		}
		let deadline = self.clock.instant() + timeout.to_std();
		loop {
			if self.flow_consumer() >= version {
				return true;
			}
			if self.clock.instant() >= deadline {
				return self.flow_consumer() >= version;
			}
			thread::sleep(Duration::from_milliseconds(2).unwrap().to_std());
		}
	}
}

pub struct FlowWatermarks<'a> {
	source: FlowWatermarkSampler,
	_marker: PhantomData<&'a Database>,
}

impl FlowWatermarks<'_> {
	pub fn all(&self) -> Vec<FlowWatermarkRow> {
		self.source.all()
	}
}

pub struct SubscriptionWatermarks<'a> {
	source: SubscriptionWatermarkSampler,
	_marker: PhantomData<&'a Database>,
}

impl SubscriptionWatermarks<'_> {
	pub fn all(&self) -> Vec<SubscriptionWatermarkRow> {
		self.source.all()
	}
}

#[cfg(feature = "sub_replication")]
pub struct ReplicaWatermarks<'a> {
	watermark: ReplicaWatermark,
	_marker: PhantomData<&'a Database>,
}

#[cfg(feature = "sub_replication")]
impl ReplicaWatermarks<'_> {
	/// Last commit version successfully applied by the replica applier.
	pub fn current(&self) -> CommitVersion {
		self.watermark.get()
	}
}

#[derive(Debug, Clone)]
pub struct WatermarkSnapshot {
	pub tx: TxSnapshot,
	pub cdc: CdcSnapshot,
	pub flow: Option<Vec<FlowWatermarkRow>>,
	pub replica: Option<CommitVersion>,
}

#[derive(Debug, Clone, Copy)]
pub struct TxSnapshot {
	pub current: CommitVersion,
	pub done_until: CommitVersion,
}

#[derive(Debug, Clone, Copy)]
pub struct CdcSnapshot {
	pub producer: CommitVersion,
	pub max: CommitVersion,
	pub consumer: CommitVersion,
}
