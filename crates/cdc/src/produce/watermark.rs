// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeSet,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_core::common::CommitVersion;
use reifydb_runtime::{reifydb_assertions, sync::mutex::Mutex};

#[derive(Clone, Default)]
pub struct CdcProducerWatermark {
	published: Arc<AtomicU64>,
	state: Arc<Mutex<WatermarkState>>,
}

#[derive(Default)]
struct WatermarkState {
	hi: Option<u64>,
	pending: BTreeSet<u64>,
}

impl CdcProducerWatermark {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn advance(&self, version: CommitVersion) {
		let v = version.0;
		let mut state = self.state.lock();
		match state.hi {
			Some(hi) if v <= hi => return,
			Some(hi) if v > hi + 1 => {
				state.pending.insert(v);
				return;
			}
			_ => {}
		}
		let mut hi = v;
		while state.pending.remove(&(hi + 1)) {
			hi += 1;
		}
		reifydb_assertions! {
			let prev = self.published.load(Ordering::SeqCst);
			assert!(
				hi >= prev,
				"contiguous advancement produced a watermark below the one already published, so a consumer \
				 bounded by it would rewind and re-read CDC it already consumed; an out-of-order version was \
				 published instead of buffered (advancing v={v}, already published prev={prev}, recomputed hi={hi})"
			);
		}
		state.hi = Some(hi);
		self.published.store(hi, Ordering::SeqCst);
	}

	pub fn get(&self) -> CommitVersion {
		CommitVersion(self.published.load(Ordering::SeqCst))
	}
}

#[cfg(test)]
mod tests {
	use std::thread;

	use super::*;

	fn wm() -> CdcProducerWatermark {
		CdcProducerWatermark::new()
	}

	fn v(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	#[test]
	fn get_is_zero_before_any_advance() {
		// Nothing emitted yet, so no CDC is visible.
		assert_eq!(wm().get(), v(0));
	}

	#[test]
	fn in_order_advances_track_each_version() {
		let w = wm();
		w.advance(v(1));
		assert_eq!(w.get(), v(1));
		w.advance(v(2));
		assert_eq!(w.get(), v(2));
		w.advance(v(3));
		assert_eq!(w.get(), v(3));
	}

	#[test]
	fn first_advance_establishes_an_arbitrary_base() {
		// The producer starts after bootstrap, so the first version it sees is
		// not necessarily 1; the first advance establishes the contiguous base.
		let w = wm();
		w.advance(v(7));
		assert_eq!(w.get(), v(7));
		w.advance(v(8));
		assert_eq!(w.get(), v(8));
	}

	#[test]
	fn out_of_order_version_is_buffered_until_the_gap_fills() {
		let w = wm();
		w.advance(v(1));
		w.advance(v(3));
		assert_eq!(w.get(), v(1), "must not advance past the unfilled gap at 2");
		w.advance(v(2));
		assert_eq!(w.get(), v(3), "filling 2 must drain the buffered 3");
	}

	#[test]
	fn out_of_order_at_the_base_is_buffered() {
		let w = wm();
		w.advance(v(7));
		assert_eq!(w.get(), v(7));
		w.advance(v(9));
		assert_eq!(w.get(), v(7), "8 is missing");
		w.advance(v(8));
		assert_eq!(w.get(), v(9));
	}

	#[test]
	fn filling_one_gap_drains_a_whole_contiguous_run() {
		let w = wm();
		w.advance(v(1));
		w.advance(v(4));
		w.advance(v(3));
		w.advance(v(5));
		assert_eq!(w.get(), v(1), "2 still missing keeps everything buffered");
		w.advance(v(2));
		assert_eq!(w.get(), v(5), "2 fills -> 3,4,5 all become contiguous");
	}

	#[test]
	fn the_deferred_join_stall_scenario() {
		// Regression for the deferred-flow stall: concurrent commits race through
		// the post-commit listener, so the producer can process 13 before 12. The
		// watermark must NOT report 13 while 12's CDC is unwritten; otherwise a
		// consumer reads (11,13], finds no CDC for the still-pending 12, and skips
		// its checkpoint past the insert, losing it forever.
		let w = wm();
		for n in 7..=11 {
			w.advance(v(n));
		}
		assert_eq!(w.get(), v(11));
		w.advance(v(13));
		assert_eq!(w.get(), v(11), "must stay at 11 while 12 is a gap, not jump to 13");
		w.advance(v(12));
		assert_eq!(w.get(), v(13), "once 12 is visible, 12 and 13 are both contiguous");
	}

	#[test]
	fn duplicate_and_below_current_advances_are_noops() {
		let w = wm();
		w.advance(v(5));
		w.advance(v(6));
		assert_eq!(w.get(), v(6));
		w.advance(v(6));
		w.advance(v(4));
		w.advance(v(1));
		assert_eq!(w.get(), v(6), "re-advancing at or below the contiguous hi never moves it");
	}

	#[test]
	fn duplicate_of_a_buffered_version_is_idempotent() {
		let w = wm();
		w.advance(v(1));
		w.advance(v(3));
		w.advance(v(3));
		assert_eq!(w.get(), v(1));
		w.advance(v(2));
		assert_eq!(w.get(), v(3));
	}

	#[test]
	fn get_never_reports_a_buffered_non_contiguous_version() {
		let w = wm();
		w.advance(v(1));
		w.advance(v(100));
		assert_eq!(w.get(), v(1), "100 is buffered, get() must report only contiguous progress");
		for n in 2..=99 {
			w.advance(v(n));
		}
		assert_eq!(w.get(), v(100));
	}

	#[test]
	fn large_reverse_run_buffers_then_fills() {
		let w = wm();
		w.advance(v(1));
		for n in (3..=10).rev() {
			w.advance(v(n));
		}
		assert_eq!(w.get(), v(1));
		w.advance(v(2));
		assert_eq!(w.get(), v(10));
	}

	#[test]
	fn interleaved_in_and_out_of_order() {
		let w = wm();
		w.advance(v(1));
		w.advance(v(2));
		w.advance(v(5));
		w.advance(v(3));
		assert_eq!(w.get(), v(3), "4 missing, 5 buffered");
		w.advance(v(7));
		assert_eq!(w.get(), v(3));
		w.advance(v(4));
		assert_eq!(w.get(), v(5), "4 fills -> 5 drains, 6 still missing");
		w.advance(v(6));
		assert_eq!(w.get(), v(7), "6 fills -> 7 drains");
	}

	#[test]
	fn concurrent_shuffled_advances_converge_to_the_full_contiguous_max() {
		let w = wm();
		w.advance(v(1));

		let mut handles = Vec::new();
		for chunk_start in (2..=200).step_by(20) {
			let w = w.clone();
			handles.push(thread::spawn(move || {
				let end = (chunk_start + 19).min(200);
				for n in (chunk_start..=end).rev() {
					w.advance(v(n));
				}
			}));
		}
		for h in handles {
			h.join().unwrap();
		}

		assert_eq!(w.get(), v(200), "once every version 1..=200 is advanced, get() is the full contiguous max");
	}
}
