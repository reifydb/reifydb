// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Garbage collection. Three reclamation strategies cover the cases the multi-version store generates: historical
//! reclaims versions older than the active read watermark; row reclaims tombstones once no reader can see the
//! pre-tombstone version; operator handles per-flow retention overrides where some operators keep less history
//! than the global default.

pub mod epoch;
pub mod historical;
pub mod operator;
pub mod ringbuffer;
pub mod row;

use reifydb_core::common::CommitVersion;

pub trait EvictionWatermark: Send + Sync + 'static {
	fn watermark(&self) -> CommitVersion;
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Mirrors the engine's EngineEvictionWatermark::eviction_cutoff intent: the cutoff is the MINIMUM of the
	/// query watermark and the consumer watermark, so the more conservative (lower) reader holds eviction back.
	/// The store-multi crate cannot construct the real StandardEngine-backed watermark, so this pins the
	/// min-of-two contract directly.
	struct MinWatermark {
		query: CommitVersion,
		consumer: CommitVersion,
	}

	impl EvictionWatermark for MinWatermark {
		fn watermark(&self) -> CommitVersion {
			self.query.min(self.consumer)
		}
	}

	#[test]
	fn cutoff_is_the_min_of_query_and_consumer_watermarks() {
		// A held-back consumer (lower watermark) must lower the cutoff: data the consumer can still reach
		// stays resident. The lower of the two always wins, regardless of which side is lower.
		let consumer_behind = MinWatermark {
			query: CommitVersion(100),
			consumer: CommitVersion(40),
		};
		assert_eq!(
			consumer_behind.watermark(),
			CommitVersion(40),
			"a consumer lagging behind the query watermark must pull the cutoff down to its position"
		);

		let query_behind = MinWatermark {
			query: CommitVersion(30),
			consumer: CommitVersion(90),
		};
		assert_eq!(
			query_behind.watermark(),
			CommitVersion(30),
			"a lagging query watermark must pull the cutoff down too - the min is symmetric"
		);
	}
}
