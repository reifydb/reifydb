// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Retention classes and the floor terms that constrain them (decision B3).
//!
//! Every class of reclaimable data answers two questions here: WHO can still read the data this class deletes, and
//! WHICH floor term protects that reader. A class missing a term it needs is a correctness bug - it deletes data a
//! live reader still resolves. A class carrying a term it does not need is a liveness bug - one wedged reader
//! freezes reclamation that never concerned it, which is how a single stalled CDC consumer used to pin the entire
//! store.
//!
//! The terms are a type, not documentation, so adding a class forces an explicit answer and changing a floor fails
//! the test that names the reader it was protecting.

use std::fmt;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FloorTerm {
	RowExpiry,

	OperatorExpiry,

	QueryDoneUntil,

	LeaseMin,

	ConsumerCheckpoint,

	SubscriptionSnapshot,

	FlushWatermark,

	OwningFlowCheckpoint,

	RetentionHorizon,
}

impl FloorTerm {
	pub fn protects(&self) -> &'static str {
		match self {
			Self::RowExpiry => "rows younger than their declared ttl",
			Self::OperatorExpiry => "operator state younger than its declared ttl",
			Self::QueryDoneUntil => "an in-flight query reading at its snapshot version",
			Self::LeaseMin => "a held operator-state lease",
			Self::ConsumerCheckpoint => "a CDC log consumer that has not yet consumed the version",
			Self::SubscriptionSnapshot => {
				"a lagging subscription worker still reading rows at its own position"
			}
			Self::FlushWatermark => "a write that has not yet reached the persistent tier",
			Self::OwningFlowCheckpoint => "the owning flow's own unprocessed input",
			Self::RetentionHorizon => "epoch samples still needed to resolve the longest declared ttl",
		}
	}
}

impl fmt::Display for FloorTerm {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::RowExpiry => write!(f, "row-expiry"),
			Self::OperatorExpiry => write!(f, "operator-expiry"),
			Self::QueryDoneUntil => write!(f, "query-done-until"),
			Self::LeaseMin => write!(f, "lease-min"),
			Self::ConsumerCheckpoint => write!(f, "consumer-checkpoint"),
			Self::SubscriptionSnapshot => write!(f, "subscription-snapshot"),
			Self::FlushWatermark => write!(f, "flush-watermark"),
			Self::OwningFlowCheckpoint => write!(f, "owning-flow-checkpoint"),
			Self::RetentionHorizon => write!(f, "retention-horizon"),
		}
	}
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RetentionClass {
	RowTtlDrop,

	RowTtlDelete,

	OperatorTtl,

	BufferHistoricalGc,

	PersistentFlush,

	PendingDropsPurge,

	CdcTruncate,

	EpochLog,
}

impl RetentionClass {
	pub fn all() -> &'static [Self] {
		&[
			Self::RowTtlDrop,
			Self::RowTtlDelete,
			Self::OperatorTtl,
			Self::BufferHistoricalGc,
			Self::PersistentFlush,
			Self::PendingDropsPurge,
			Self::CdcTruncate,
			Self::EpochLog,
		]
	}

	pub fn name(&self) -> &'static str {
		match self {
			Self::RowTtlDrop => "row-ttl-drop",
			Self::RowTtlDelete => "row-ttl-delete",
			Self::OperatorTtl => "operator-ttl",
			Self::BufferHistoricalGc => "buffer-historical-gc",
			Self::PersistentFlush => "persistent-flush",
			Self::PendingDropsPurge => "pending-drops-purge",
			Self::CdcTruncate => "cdc-truncate",
			Self::EpochLog => "epoch-log",
		}
	}

	pub fn floor_terms(&self) -> &'static [FloorTerm] {
		match self {
			Self::RowTtlDrop => &[FloorTerm::RowExpiry],
			Self::RowTtlDelete => &[FloorTerm::RowExpiry],
			Self::OperatorTtl => &[FloorTerm::OperatorExpiry],
			Self::BufferHistoricalGc => {
				&[FloorTerm::QueryDoneUntil, FloorTerm::LeaseMin, FloorTerm::SubscriptionSnapshot]
			}
			Self::PersistentFlush => &[
				FloorTerm::QueryDoneUntil,
				FloorTerm::LeaseMin,
				FloorTerm::SubscriptionSnapshot,
				FloorTerm::ConsumerCheckpoint,
			],
			Self::PendingDropsPurge => &[FloorTerm::FlushWatermark],
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
	fn every_class_declares_at_least_one_floor_term() {
		// A class with no floor deletes at head version: whatever it owns, it deletes immediately on write.
		// That is never a legitimate policy, so an empty term list is a construction error rather than a
		// permissive default.
		for class in RetentionClass::all() {
			assert!(
				!class.floor_terms().is_empty(),
				"{class} declares no floor term, so nothing bounds what it deletes"
			);
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
		// This is the core of decision B3 and the fix for the incident that motivated it: a wedged CDC
		// consumer or a leaked query lease used to freeze row expiry through one shared watermark. Row
		// expiry is protected by MVCC-transactional discovery, not by those readers, so those terms must
		// stay out of its floor.
		for class in [RetentionClass::RowTtlDrop, RetentionClass::RowTtlDelete] {
			assert!(
				!class.constrained_by(FloorTerm::ConsumerCheckpoint),
				"{class} must not be pinned by a CDC consumer; it reclaims rows no consumer reads"
			);
			assert!(
				!class.constrained_by(FloorTerm::SubscriptionSnapshot),
				"{class} must not be pinned by a lagging subscription worker"
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
		// The mirror image of row expiry: buffer history IS what a lagging reader resolves against, so all
		// three reader terms must be present. Dropping one silently loses a row mid-read rather than stalling.
		let class = RetentionClass::BufferHistoricalGc;

		for term in [FloorTerm::QueryDoneUntil, FloorTerm::LeaseMin, FloorTerm::SubscriptionSnapshot] {
			assert!(
				class.constrained_by(term),
				"{class} must keep the {term} term: it protects {}",
				term.protects()
			);
		}
	}

	#[test]
	fn a_subscription_reading_history_is_a_different_reader_from_a_cdc_log_consumer() {
		// These two were one term until the distinction was traced through the code, and conflating them
		// points the floor matrix at the wrong reader in both directions.
		//
		// A CDC LOG consumer reads cdc.db and needs no multi-store history - it constrains CDC truncation
		// only. A SUBSCRIPTION worker is different: for every batch it leases its lag position and reads rows
		// at that version out of the multi store (sub-subscription worker/dispatch.rs), so it genuinely pins
		// buffer history. Removing that term makes a lagging subscription fail its lease acquire with
		// TXN_012, which is a production incident, not a liveness win.
		assert!(
			RetentionClass::BufferHistoricalGc.constrained_by(FloorTerm::SubscriptionSnapshot),
			"a lagging subscription reads buffer history at its own position and must pin it"
		);
		assert!(
			!RetentionClass::BufferHistoricalGc.constrained_by(FloorTerm::ConsumerCheckpoint),
			"a CDC log consumer reads cdc.db, not buffer history, and must not pin it"
		);
		assert!(
			!RetentionClass::CdcTruncate.constrained_by(FloorTerm::SubscriptionSnapshot),
			"a subscription's snapshot position does not govern how far cdc.db may truncate"
		);
	}

	#[test]
	fn cdc_truncation_is_pinned_only_by_its_consumers() {
		// CDC genuinely must respect the slowest consumer - that term is correct here, unlike in row
		// expiry. But it must not additionally inherit query or lease terms, or an unrelated stuck query
		// would stop cdc.db from ever shrinking.
		let class = RetentionClass::CdcTruncate;

		assert!(
			class.constrained_by(FloorTerm::ConsumerCheckpoint),
			"the slowest CDC log consumer legitimately pins CDC"
		);
		assert!(!class.constrained_by(FloorTerm::QueryDoneUntil), "an in-flight query does not read the CDC log");
		assert!(!class.constrained_by(FloorTerm::LeaseMin), "an operator-state lease does not read the CDC log");
	}

	#[test]
	fn pending_drop_purges_wait_for_the_flush_that_could_resurrect_them() {
		// The persistent tier is version-guarded single-version-per-key: purging a key whose superseding
		// write has not flushed lets the stale flush write it back. That is the resurrection bug, and this
		// term is the thing preventing it.
		assert!(
			RetentionClass::PendingDropsPurge.constrained_by(FloorTerm::FlushWatermark),
			"a pending drop purged before its flush is durable can be resurrected by that flush"
		);
	}

	#[test]
	fn the_epoch_log_is_bounded_by_the_longest_ttl_it_must_still_answer() {
		// Pruning epoch samples below the longest declared ttl would make that ttl unresolvable: the cutoff
		// silently becomes none and the data it governs never expires. That is the original defect, so the
		// horizon term is what keeps the epoch log from re-creating it.
		assert!(
			RetentionClass::EpochLog.constrained_by(FloorTerm::RetentionHorizon),
			"pruning epoch samples inside the retention horizon would make long ttls unresolvable"
		);
	}
}
