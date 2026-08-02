// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_types)]

use std::{
	cell::UnsafeCell,
	hint::spin_loop,
	ptr::null_mut,
	sync::atomic::{AtomicPtr, AtomicU32, Ordering},
	thread::{Thread, current, park_timeout},
	time::Duration,
};
#[cfg(not(reifydb_single_threaded))]
use std::{
	sync::{Arc, OnceLock, atomic::AtomicBool},
	thread::{Builder, JoinHandle, park},
};

use reifydb_core::common::CommitVersion;
use reifydb_value::{Result, reifydb_assertions};

use super::{CommitShared, CreateCommitResult, OracleState, SpanTiming};
use crate::multi::{conflict::ConflictManager, transaction::version::VersionProvider};

const QUEUED: u32 = 0;
const DONE: u32 = 1;
const SPIN_ROUNDS: u32 = 16;
const WAITER_PARK: Duration = Duration::from_micros(50);

struct CommitPayload {
	version: CommitVersion,
	window_size: u64,
	conflicts: Option<ConflictManager>,
	result: Option<Result<CreateCommitResult>>,
}

struct CommitRequest {
	payload: UnsafeCell<CommitPayload>,
	state: AtomicU32,
	waiter: Thread,
	next: AtomicPtr<CommitRequest>,
}

// SAFETY: payload is accessed exclusively by the enqueuing thread before push and after it
// observes DONE with Acquire ordering, and exclusively by the servicing thread between its
// Acquire drain of the queue and its Release store of DONE; state, waiter and next are Sync.
unsafe impl Sync for CommitRequest {}

impl CommitRequest {
	fn new(version: CommitVersion, window_size: u64, conflicts: ConflictManager) -> Self {
		Self {
			payload: UnsafeCell::new(CommitPayload {
				version,
				window_size,
				conflicts: Some(conflicts),
				result: None,
			}),
			state: AtomicU32::new(QUEUED),
			waiter: current(),
			next: AtomicPtr::new(null_mut()),
		}
	}

	fn done(&self) -> bool {
		self.state.load(Ordering::Acquire) == DONE
	}

	fn take_result(&self) -> Result<CreateCommitResult> {
		// SAFETY: DONE was observed with Acquire ordering, so the servicing thread's writes
		// to payload happened-before this access and it never touches the request again.
		let payload = unsafe { &mut *self.payload.get() };
		payload.result.take().expect("a serviced commit request must carry a result")
	}
}

fn complete_request(request: *mut CommitRequest, result: Result<CreateCommitResult>) {
	// SAFETY: the enqueuing thread keeps the request alive and does not touch its payload
	// until it observes the DONE stored below; the waiter handle is cloned out before DONE
	// is published, after which the request memory may be reclaimed at any moment.
	let payload = unsafe { &mut *(*request).payload.get() };
	payload.result = Some(result);
	let waiter = unsafe { (*request).waiter.clone() };
	unsafe { (*request).state.store(DONE, Ordering::Release) };
	waiter.unpark();
}

pub(super) struct CommitQueue {
	head: AtomicPtr<CommitRequest>,
	#[cfg(not(reifydb_single_threaded))]
	sequencer_parked: AtomicBool,
	#[cfg(not(reifydb_single_threaded))]
	sequencer_active: AtomicBool,
	#[cfg(not(reifydb_single_threaded))]
	sequencer_stop: AtomicBool,
	#[cfg(not(reifydb_single_threaded))]
	sequencer_thread: OnceLock<Thread>,
}

impl CommitQueue {
	pub(super) fn new() -> Self {
		Self {
			head: AtomicPtr::new(null_mut()),
			#[cfg(not(reifydb_single_threaded))]
			sequencer_parked: AtomicBool::new(false),
			#[cfg(not(reifydb_single_threaded))]
			sequencer_active: AtomicBool::new(false),
			#[cfg(not(reifydb_single_threaded))]
			sequencer_stop: AtomicBool::new(false),
			#[cfg(not(reifydb_single_threaded))]
			sequencer_thread: OnceLock::new(),
		}
	}

	fn push(&self, request: *mut CommitRequest) {
		let mut head = self.head.load(Ordering::Relaxed);
		loop {
			// SAFETY: the request is not yet visible to any other thread, so storing its
			// next pointer cannot race.
			unsafe { (*request).next.store(head, Ordering::Relaxed) };
			match self.head.compare_exchange_weak(head, request, Ordering::SeqCst, Ordering::Relaxed) {
				Ok(_) => return,
				Err(observed) => head = observed,
			}
		}
	}

	fn drain_fifo(&self) -> *mut CommitRequest {
		let mut lifo = self.head.swap(null_mut(), Ordering::Acquire);
		let mut fifo = null_mut();
		while !lifo.is_null() {
			// SAFETY: after the swap this thread exclusively owns the drained list, and
			// every request in it is kept alive by its enqueuing thread until DONE.
			let next = unsafe { (*lifo).next.load(Ordering::Relaxed) };
			// SAFETY: same exclusive ownership of the drained list as above.
			unsafe { (*lifo).next.store(fifo, Ordering::Relaxed) };
			fifo = lifo;
			lifo = next;
		}
		fifo
	}

	#[cfg_attr(reifydb_single_threaded, allow(dead_code))]
	fn is_empty(&self) -> bool {
		self.head.load(Ordering::SeqCst).is_null()
	}

	#[cfg(not(reifydb_single_threaded))]
	fn sequencer_active(&self) -> bool {
		self.sequencer_active.load(Ordering::Relaxed)
	}

	#[cfg(reifydb_single_threaded)]
	fn sequencer_active(&self) -> bool {
		false
	}

	#[cfg(not(reifydb_single_threaded))]
	fn sequencer_stopped(&self) -> bool {
		self.sequencer_stop.load(Ordering::SeqCst)
	}

	#[cfg(reifydb_single_threaded)]
	fn sequencer_stopped(&self) -> bool {
		false
	}

	#[cfg(not(reifydb_single_threaded))]
	fn wake_sequencer(&self) {
		if self.sequencer_parked.swap(false, Ordering::SeqCst)
			&& let Some(sequencer) = self.sequencer_thread.get()
		{
			sequencer.unpark();
		}
	}

	#[cfg(reifydb_single_threaded)]
	fn wake_sequencer(&self) {}

	#[cfg(not(reifydb_single_threaded))]
	pub(super) fn stop_sequencer(&self) {
		self.sequencer_stop.store(true, Ordering::SeqCst);
		self.sequencer_parked.store(false, Ordering::SeqCst);
		if let Some(sequencer) = self.sequencer_thread.get() {
			sequencer.unpark();
		}
	}
}

#[cfg(not(reifydb_single_threaded))]
pub(super) fn spawn_sequencer<L>(shared: &Arc<CommitShared<L>>) -> JoinHandle<()>
where
	L: VersionProvider + 'static,
{
	let shared = Arc::clone(shared);
	Builder::new()
		.name("txn-oracle-commit".into())
		.spawn(move || shared.sequencer_loop())
		.expect("spawning the oracle commit sequencer thread must succeed")
}

impl<L> CommitShared<L>
where
	L: VersionProvider,
{
	pub(super) fn commit(
		&self,
		version: CommitVersion,
		conflicts: ConflictManager,
		window_size: u64,
	) -> Result<CreateCommitResult> {
		let request = CommitRequest::new(version, window_size, conflicts);
		self.queue.push(&request as *const CommitRequest as *mut CommitRequest);
		loop {
			if self.queue.sequencer_active() {
				self.queue.wake_sequencer();
			} else if let Some(mut state) = self.inner.try_write() {
				self.service_queue(&mut state);
				drop(state);
				reifydb_assertions! {
					assert!(
						request.done(),
						"a committer that drained the queue under the exclusive lock \
						 must have serviced its own request"
					);
				}
			} else {
				self.queue.wake_sequencer();
			}
			if request.done() {
				return request.take_result();
			}
			for _ in 0..SPIN_ROUNDS {
				spin_loop();
				if request.done() {
					return request.take_result();
				}
			}
			if self.queue.sequencer_stopped() {
				let mut state = self.inner.write();
				self.service_queue(&mut state);
			}
			if request.done() {
				return request.take_result();
			}
			park_timeout(WAITER_PARK);
		}
	}

	fn service_queue(&self, state: &mut OracleState) {
		let timing = SpanTiming::current();
		loop {
			let mut request = self.queue.drain_fifo();
			if request.is_null() {
				return;
			}
			while !request.is_null() {
				// SAFETY: the enqueuing thread keeps the request alive and does not
				// touch its payload until it observes DONE; until then this thread
				// has exclusive access to the payload.
				let next = unsafe { (*request).next.load(Ordering::Relaxed) };
				let payload = unsafe { &mut *(*request).payload.get() };
				let conflicts = payload
					.conflicts
					.take()
					.expect("a queued commit request must carry a conflict manager");
				let result =
					self.service(state, payload.version, conflicts, payload.window_size, timing);
				complete_request(request, result);
				request = next;
			}
		}
	}

	#[cfg(not(reifydb_single_threaded))]
	fn sequencer_loop(&self) {
		let _ = self.queue.sequencer_thread.set(current());
		loop {
			if self.queue.sequencer_stop.load(Ordering::SeqCst) {
				let mut state = self.inner.write();
				self.service_queue(&mut state);
				return;
			}
			if self.queue.is_empty() {
				self.queue.sequencer_parked.store(true, Ordering::SeqCst);
				if self.queue.is_empty() && !self.queue.sequencer_stop.load(Ordering::SeqCst) {
					park();
				}
				self.queue.sequencer_parked.store(false, Ordering::SeqCst);
				continue;
			}
			self.queue.sequencer_active.store(true, Ordering::Relaxed);
			let mut state = self.inner.write();
			self.service_queue(&mut state);
			drop(state);
			self.queue.sequencer_active.store(false, Ordering::Relaxed);
		}
	}
}
