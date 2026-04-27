// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Unified watermark / progress accessors for a `Database`.
//!
//! Each domain (transaction, CDC, flow, replica) is exposed under
//! `db.watermarks().<domain>()`, returning a borrowed view that reads the
//! underlying value lazily. `db.watermarks().snapshot()` reads all of them
//! in one call for telemetry use.

use std::marker::PhantomData;

use reifydb_core::{
	common::CommitVersion,
	interface::flow::{FlowWatermarkRow, FlowWatermarkSampler},
};
#[cfg(feature = "sub_replication")]
use reifydb_sub_replication::replica::watermark::ReplicaWatermark;
use reifydb_type::Result;

use crate::Database;

/// Borrowed accessor returned by `Database::watermarks()`.
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
		}
	}

	pub fn flow(&self) -> Option<FlowWatermarks<'a>> {
		let source = self.db.engine().ioc().resolve::<FlowWatermarkSampler>().ok()?;
		Some(FlowWatermarks {
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

	/// Read every watermark in one call. Errors propagate from `tx().current()`.
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
}

impl CdcWatermarks<'_> {
	/// Highest commit version processed by the CDC producer. Advances even for
	/// commits whose deltas are entirely filtered out by `should_exclude_from_cdc`,
	/// so it is the correct frontier for "producer is caught up to the engine".
	pub fn producer(&self) -> CommitVersion {
		self.db.engine().cdc_producer_watermark()
	}

	/// Largest version that has a row in the CDC store. Permanently lags by
	/// the number of commits whose deltas were entirely excluded from CDC
	/// (e.g. `ConfigStorage`-only commits); use `producer()` to ask "is the
	/// producer caught up?".
	pub fn max(&self) -> CommitVersion {
		self.db.engine().cdc_store().max_version().ok().flatten().unwrap_or(CommitVersion(0))
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
}
