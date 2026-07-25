// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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

	pub fn all() -> &'static [Self] {
		&[
			Self::RowExpiry,
			Self::OperatorExpiry,
			Self::QueryDoneUntil,
			Self::LeaseMin,
			Self::ConsumerCheckpoint,
			Self::SubscriptionSnapshot,
			Self::FlushWatermark,
			Self::OwningFlowCheckpoint,
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
	RowTtlSilent,

	RowTtlAnnounced,

	OperatorGroupData,

	OperatorGroupIdentity,

	BufferHistoricalGc,

	PersistentFlush,

	CompactionReclaim,

	TombstoneReap,

	VacuumBudget,

	CdcTruncate,

	EpochLog,
}

impl RetentionClass {
	pub fn all() -> &'static [Self] {
		&[
			Self::RowTtlSilent,
			Self::RowTtlAnnounced,
			Self::OperatorGroupData,
			Self::OperatorGroupIdentity,
			Self::BufferHistoricalGc,
			Self::PersistentFlush,
			Self::CompactionReclaim,
			Self::TombstoneReap,
			Self::VacuumBudget,
			Self::CdcTruncate,
			Self::EpochLog,
		]
	}

	pub fn name(&self) -> &'static str {
		match self {
			Self::RowTtlSilent => "row-ttl-silent",
			Self::RowTtlAnnounced => "row-ttl-announced",
			Self::OperatorGroupData => "operator-group-data",
			Self::OperatorGroupIdentity => "operator-group-identity",
			Self::BufferHistoricalGc => "buffer-historical-gc",
			Self::PersistentFlush => "persistent-flush",
			Self::CompactionReclaim => "pending-drops-purge",
			Self::TombstoneReap => "tombstone-reap",
			Self::VacuumBudget => "vacuum-budget",
			Self::CdcTruncate => "cdc-truncate",
			Self::EpochLog => "epoch-log",
		}
	}

	pub fn reclaims_versioned_data(&self) -> bool {
		match self {
			Self::RowTtlSilent
			| Self::RowTtlAnnounced
			| Self::OperatorGroupData
			| Self::OperatorGroupIdentity
			| Self::BufferHistoricalGc
			| Self::PersistentFlush
			| Self::CompactionReclaim
			| Self::TombstoneReap
			| Self::CdcTruncate
			| Self::EpochLog => true,
			Self::VacuumBudget => false,
		}
	}

	pub fn floor_terms(&self) -> &'static [FloorTerm] {
		match self {
			Self::RowTtlSilent => &[FloorTerm::RowExpiry],
			Self::RowTtlAnnounced => &[FloorTerm::RowExpiry],
			Self::OperatorGroupData => &[FloorTerm::OperatorExpiry, FloorTerm::OwningFlowCheckpoint],
			Self::OperatorGroupIdentity => &[FloorTerm::RowExpiry, FloorTerm::OwningFlowCheckpoint],
			Self::BufferHistoricalGc => {
				&[FloorTerm::QueryDoneUntil, FloorTerm::LeaseMin, FloorTerm::SubscriptionSnapshot]
			}
			Self::PersistentFlush => &[
				FloorTerm::QueryDoneUntil,
				FloorTerm::LeaseMin,
				FloorTerm::SubscriptionSnapshot,
				FloorTerm::ConsumerCheckpoint,
			],
			Self::CompactionReclaim => &[FloorTerm::FlushWatermark],
			Self::TombstoneReap => &[FloorTerm::FlushWatermark],
			Self::VacuumBudget => &[],
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
		// A class that reclaims versioned data and declares no floor deletes at head version: whatever it
		// owns, it deletes immediately on write. That is never legitimate, so an empty term list is a
		// construction error rather than a permissive default.
		//
		// The converse pins the exemption so it cannot be used as a hiding place. A class reclaiming only
		// space already freed by other classes (vacuum relocating pages that are on the freelist) touches no
		// row, version or tombstone, so no version bounds it and there is no honest term to name. Claiming
		// one anyway would be a lie in the matrix and in the boot report, which is exactly what the other
		// tests here exist to prevent. Deleting versioned data under that exemption is the failure this
		// direction catches.
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
	fn both_group_phases_wait_for_the_flow_that_owns_the_state() {
		// Group state belongs to one flow, and that flow may still hold unprocessed input that writes
		// to the very group being reclaimed. Neither phase may run ahead of it, so both name the term.
		// This is also the term that used to resolve to None, which is why it is asserted rather than
		// assumed.
		for class in [RetentionClass::OperatorGroupData, RetentionClass::OperatorGroupIdentity] {
			assert!(
				class.constrained_by(FloorTerm::OwningFlowCheckpoint),
				"{class} would reclaim state the owning flow has not finished writing to"
			);
		}
	}

	#[test]
	fn the_two_group_phases_are_bounded_by_different_lifetimes() {
		// The phases exist because the two halves of a group die at different times. Data is dead once
		// the operator's own horizon passes it. Identity - the row-number mapping - stays reachable for
		// as long as a sink row can still name it, which is the ROW ttl, not the operator's. Giving
		// identity the operator term would drop the mapping while a live sink row still points at it,
		// and the next write would mint a second row number for a row that already exists.
		assert!(RetentionClass::OperatorGroupData.constrained_by(FloorTerm::OperatorExpiry));
		assert!(!RetentionClass::OperatorGroupData.constrained_by(FloorTerm::RowExpiry));

		assert!(RetentionClass::OperatorGroupIdentity.constrained_by(FloorTerm::RowExpiry));
		assert!(
			!RetentionClass::OperatorGroupIdentity.constrained_by(FloorTerm::OperatorExpiry),
			"identity outlives the data it identifies; binding it to the operator horizon would \
			 collapse the two phases into one"
		);
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
		for class in [RetentionClass::RowTtlSilent, RetentionClass::RowTtlAnnounced] {
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
	fn pending_drop_purges_wait_for_the_flush_that_could_resurrect_them() {
		// The persistent tier is version-guarded single-version-per-key: purging a key whose superseding
		// write has not flushed lets the stale flush write it back. That is the resurrection bug, and this
		// term is the thing preventing it.
		assert!(
			RetentionClass::CompactionReclaim.constrained_by(FloorTerm::FlushWatermark),
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
