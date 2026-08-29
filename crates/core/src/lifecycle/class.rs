// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

use reifydb_value::value::datetime::DateTime;

use crate::common::CommitVersion;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Floor {
	Version(CommitVersion),
	Instant(DateTime),
}

impl Floor {
	pub fn monotonic_key(&self) -> u64 {
		match self {
			Self::Version(version) => version.0,
			Self::Instant(instant) => instant.to_nanos(),
		}
	}

	pub fn version(&self) -> Option<CommitVersion> {
		match self {
			Self::Version(version) => Some(*version),
			Self::Instant(_) => None,
		}
	}

	pub fn instant(&self) -> Option<DateTime> {
		match self {
			Self::Instant(instant) => Some(*instant),
			Self::Version(_) => None,
		}
	}

	pub fn is_same_domain(&self, other: &Self) -> bool {
		matches!((self, other), (Self::Version(_), Self::Version(_)) | (Self::Instant(_), Self::Instant(_)))
	}
}

impl fmt::Display for Floor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Version(version) => write!(f, "v{}", version.0),
			Self::Instant(instant) => write!(f, "{instant}"),
		}
	}
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FloorTerm {
	RowExpiry,

	QueryDoneUntil,

	LeaseMin,

	ConsumerCheckpoint,

	ConsumerPosition,

	RetentionHorizon,
}

impl FloorTerm {
	pub fn protects(&self) -> &'static str {
		match self {
			Self::RowExpiry => "rows younger than their declared ttl",
			Self::QueryDoneUntil => "an in-flight query reading at its snapshot version",
			Self::LeaseMin => "a held operator-state lease",
			Self::ConsumerCheckpoint => "a CDC log consumer that has not yet consumed the version",
			Self::ConsumerPosition => "a live flow that has not yet consumed the version",
			Self::RetentionHorizon => "epoch samples still needed to resolve the longest declared ttl",
		}
	}

	pub fn is_clock_driven(&self) -> bool {
		match self {
			Self::RowExpiry => true,
			Self::QueryDoneUntil
			| Self::LeaseMin
			| Self::ConsumerCheckpoint
			| Self::ConsumerPosition
			| Self::RetentionHorizon => false,
		}
	}

	pub fn all() -> &'static [Self] {
		&[
			Self::RowExpiry,
			Self::QueryDoneUntil,
			Self::LeaseMin,
			Self::ConsumerCheckpoint,
			Self::ConsumerPosition,
			Self::RetentionHorizon,
		]
	}

	pub fn index(&self) -> usize {
		Self::all().iter().position(|term| term == self).expect("every term is listed in FloorTerm::all")
	}

	pub fn from_index(index: usize) -> Option<Self> {
		Self::all().get(index).copied()
	}
}

impl fmt::Display for FloorTerm {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::RowExpiry => write!(f, "row-expiry"),
			Self::QueryDoneUntil => write!(f, "query-done-until"),
			Self::LeaseMin => write!(f, "lease-min"),
			Self::ConsumerCheckpoint => write!(f, "consumer-checkpoint"),
			Self::ConsumerPosition => write!(f, "consumer-position"),
			Self::RetentionHorizon => write!(f, "retention-horizon"),
		}
	}
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RetentionClass {
	RowTtl,

	BufferHistoricalGc,

	PersistentFlush,

	QueueLeaseReap,

	QueueRetention,

	CdcTruncate,

	EpochLog,
}

impl RetentionClass {
	pub fn all() -> &'static [Self] {
		&[
			Self::RowTtl,
			Self::BufferHistoricalGc,
			Self::PersistentFlush,
			Self::QueueLeaseReap,
			Self::QueueRetention,
			Self::CdcTruncate,
			Self::EpochLog,
		]
	}

	pub fn name(&self) -> &'static str {
		match self {
			Self::RowTtl => "row-ttl-silent",
			Self::BufferHistoricalGc => "buffer-historical-gc",
			Self::PersistentFlush => "persistent-flush",
			Self::QueueLeaseReap => "queue-lease-reap",
			Self::QueueRetention => "queue-retention",
			Self::CdcTruncate => "cdc-truncate",
			Self::EpochLog => "epoch-log",
		}
	}

	pub fn reclaims_versioned_data(&self) -> bool {
		match self {
			Self::RowTtl
			| Self::BufferHistoricalGc
			| Self::PersistentFlush
			| Self::CdcTruncate
			| Self::EpochLog
			| Self::QueueRetention => true,
			Self::QueueLeaseReap => false,
		}
	}

	pub fn floor_terms(&self) -> &'static [FloorTerm] {
		match self {
			Self::RowTtl => &[FloorTerm::RowExpiry],
			Self::BufferHistoricalGc => &[FloorTerm::QueryDoneUntil, FloorTerm::LeaseMin],
			Self::PersistentFlush => {
				&[FloorTerm::QueryDoneUntil, FloorTerm::LeaseMin, FloorTerm::ConsumerPosition]
			}
			Self::QueueLeaseReap => &[],
			Self::QueueRetention => &[FloorTerm::RowExpiry],
			Self::CdcTruncate => &[FloorTerm::ConsumerCheckpoint],
			Self::EpochLog => &[FloorTerm::RetentionHorizon],
		}
	}

	pub fn constrained_by(&self, term: FloorTerm) -> bool {
		self.floor_terms().contains(&term)
	}
}

impl fmt::Display for RetentionClass {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.name())
	}
}

#[cfg(test)]
mod tests {
	use super::{FloorTerm, RetentionClass};

	#[test]
	fn every_class_declares_a_floor_term_for_exactly_the_data_it_reclaims() {
		// A class reclaiming versioned data with no floor term deletes at head version. The converse
		// pins the exemption: a class touching no row, version or tombstone has no honest term to
		// name, and deleting versioned data under that exemption is what this direction catches.
		for class in RetentionClass::all() {
			if class.reclaims_versioned_data() {
				assert!(
					!class.floor_terms().is_empty(),
					"{class} declares no floor term, so nothing bounds what it deletes"
				);
			} else {
				assert!(
					class.floor_terms().is_empty(),
					"{class} reclaims no versioned data, so a version floor cannot be what bounds it"
				);
			}
		}
	}

	#[test]
	fn class_names_are_unique_so_metrics_and_reports_cannot_collide() {
		let mut names: Vec<&str> = RetentionClass::all().iter().map(|c| c.name()).collect();
		let total = names.len();
		names.sort_unstable();
		names.dedup();

		assert_eq!(names.len(), total, "two classes share a name; their metrics and report lines would merge");
	}

	#[test]
	fn row_expiry_classes_are_not_hostage_to_readers_of_other_data() {
		// Row expiry is protected by MVCC-transactional discovery, not by these readers. Sharing one
		// watermark with them lets a wedged CDC consumer or a leaked query lease freeze it.
		for class in [RetentionClass::RowTtl] {
			assert!(
				!class.constrained_by(FloorTerm::ConsumerCheckpoint),
				"{class} must not be pinned by a CDC consumer; it reclaims rows no consumer reads"
			);
			assert!(
				!class.constrained_by(FloorTerm::LeaseMin),
				"{class} must not be pinned by an operator-state lease"
			);
			assert!(
				!class.constrained_by(FloorTerm::QueryDoneUntil),
				"{class} must not be pinned by an in-flight query; transactional discovery protects it"
			);
		}
	}

	#[test]
	fn version_history_classes_respect_every_reader_of_a_snapshot() {
		// Buffer history is what a live reader resolves against, so an in-flight query and a held
		// lease must both be present. A lagging subscription rides LeaseMin through its batch lease
		// rather than holding a term of its own.
		let class = RetentionClass::BufferHistoricalGc;

		for term in [FloorTerm::QueryDoneUntil, FloorTerm::LeaseMin] {
			assert!(
				class.constrained_by(term),
				"{class} must keep the {term} term: it protects {}",
				term.protects()
			);
		}
	}

	#[test]
	fn a_lagging_subscription_must_not_pin_buffer_history_between_batches() {
		// An ephemeral reader protects its in-flight batch with a lease and is otherwise overtaken
		// loudly: a failed acquire triggers a resync, never a silent read of reclaimed history. A
		// term of its own would let a lagging worker pin buffer history without bound.
		assert!(
			!RetentionClass::BufferHistoricalGc.constrained_by(FloorTerm::ConsumerCheckpoint),
			"a CDC log consumer reads cdc.db, not buffer history, and must not pin it"
		);
		assert!(
			RetentionClass::BufferHistoricalGc.constrained_by(FloorTerm::LeaseMin),
			"an in-flight subscription batch protects its reads through its lease, so LeaseMin must stay"
		);
	}

	#[test]
	fn the_flush_floor_tracks_live_positions_while_cdc_truncation_tracks_durable_checkpoints() {
		// The commit buffer is RAM and empty after a restart, so only a live reader can be harmed by
		// flushing it. cdc.db is the opposite: a flow resumes from its durable checkpoint, so CDC below
		// that must survive even with no live reader there. Collapsing the terms stalls buffer drain.
		assert!(
			RetentionClass::PersistentFlush.constrained_by(FloorTerm::ConsumerPosition),
			"flushing the in-memory buffer may only be held back by a reader that is live now"
		);
		assert!(
			!RetentionClass::PersistentFlush.constrained_by(FloorTerm::ConsumerCheckpoint),
			"a throttled durable checkpoint lags the real read position and must not pin the buffer"
		);

		assert!(
			RetentionClass::CdcTruncate.constrained_by(FloorTerm::ConsumerCheckpoint),
			"cdc.db must retain everything a consumer would replay from after a crash"
		);
		assert!(
			!RetentionClass::CdcTruncate.constrained_by(FloorTerm::ConsumerPosition),
			"a live position is lost on restart, so it cannot govern durable CDC truncation"
		);
	}

	#[test]
	fn cdc_truncation_is_pinned_only_by_its_consumers() {
		// CDC must respect the slowest consumer, but inheriting query or lease terms would let an
		// unrelated stuck query stop cdc.db from ever shrinking.
		let class = RetentionClass::CdcTruncate;

		assert!(
			class.constrained_by(FloorTerm::ConsumerCheckpoint),
			"the slowest CDC log consumer legitimately pins CDC"
		);
		assert!(
			!class.constrained_by(FloorTerm::QueryDoneUntil),
			"an in-flight query does not read the CDC log"
		);
		assert!(
			!class.constrained_by(FloorTerm::LeaseMin),
			"an operator-state lease does not read the CDC log"
		);
	}

	#[test]
	fn the_epoch_log_is_bounded_by_the_longest_ttl_it_must_still_answer() {
		// Pruning epoch samples below the longest declared ttl makes that ttl unresolvable: the cutoff
		// silently becomes none and the data it governs never expires.
		assert!(
			RetentionClass::EpochLog.constrained_by(FloorTerm::RetentionHorizon),
			"pruning epoch samples inside the retention horizon would make long ttls unresolvable"
		);
	}
}
