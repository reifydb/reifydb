// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{
	ops::Deref,
	sync::{
		Arc,
		atomic::{AtomicPtr, Ordering},
	},
};

use crossbeam_channel::{Receiver, Sender, unbounded};
use reifydb_core::WaitGroup;

#[derive(Debug)]
struct Canceler {
	ptr: AtomicPtr<()>,
}

impl Canceler {
	fn cancel(&self) {
		// Safely take the sender out of the AtomicPtr.
		let tx_ptr = self.ptr.swap(std::ptr::null_mut(), Ordering::AcqRel);

		// Check if the pointer is not null (indicating it hasn't been
		// taken already).
		if !tx_ptr.is_null() {
			// Safe because we ensure that this is the only place
			// that takes ownership of the pointer,
			// and it is done only once.
			unsafe {
				// Convert the pointer back to a Box to take
				// ownership and drop the sender.
				let tx = Box::from_raw(tx_ptr as *mut Sender<()>);
				drop(tx);
			}
		}
	}
}

impl Drop for Canceler {
	fn drop(&mut self) {
		self.cancel();
	}
}

#[derive(Debug)]
#[repr(transparent)]
struct CancelContext {
	rx: Receiver<()>,
}

impl CancelContext {
	fn new() -> (Self, Canceler) {
		let (tx, rx) = unbounded();
		(
			Self {
				rx,
			},
			Canceler {
				ptr: AtomicPtr::new(Box::into_raw(Box::new(tx)) as _),
			},
		)
	}

	fn done(&self) -> Receiver<()> {
		self.rx.clone()
	}
}

/// Closer holds the two things we need to close a thread and wait for it to
/// finish: a chan to tell the thread to shut down, and a WaitGroup with
/// which to wait for it to finish shutting down.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Closer(Arc<CloserInner>);

impl Deref for Closer {
	type Target = CloserInner;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Debug)]
pub struct CloserInner {
	wg: WaitGroup,
	ctx: CancelContext,
	cancel: Canceler,
}

impl CloserInner {
	fn new() -> Self {
		let (ctx, cancel) = CancelContext::new();
		Self {
			wg: WaitGroup::new(),
			ctx,
			cancel,
		}
	}

	fn with(initial: usize) -> Self {
		let (ctx, cancel) = CancelContext::new();
		Self {
			wg: WaitGroup::from(initial),
			ctx,
			cancel,
		}
	}
}

impl Default for Closer {
	fn default() -> Self {
		Self(Arc::new(CloserInner::new()))
	}
}

impl Closer {
	/// Constructs a new [`Closer`], with an initial count on the
	/// [`WaitGroup`].
	pub fn new(initial: usize) -> Self {
		Self(Arc::new(CloserInner::with(initial)))
	}

	/// Calls [`WaitGroup::done`] on the [`WaitGroup`].
	pub fn done(&self) {
		self.wg.done();
	}

	/// Signals the [`Closer::has_been_closed`] signal.
	pub fn signal(&self) {
		self.cancel.cancel();
	}

	/// Waits on the [`WaitGroup`]. (It waits for the Closer's initial
	/// value, [`Closer::add_running`], and [`Closer::done`]
	pub fn wait(&self) {
		self.wg.wait();
	}

	/// Calls [`Closer::signal`], then [`Closer::wait`].
	pub fn signal_and_wait(&self) {
		self.signal();
		self.wait();
	}

	/// Listens for the [`Closer::signal`] signal.
	pub fn listen(&self) -> Receiver<()> {
		self.ctx.done()
	}
}

#[cfg(test)]
mod tests {
	use crate::mvcc::watermark::Closer;

	#[test]
	fn test_multiple_singles() {
		let closer = Closer::default();
		closer.signal();
		closer.signal();
		closer.signal_and_wait();

		let closer = Closer::new(1);
		closer.done();
		closer.signal_and_wait();
		closer.signal_and_wait();
		closer.signal();
	}

	#[test]
	fn test_closer_single() {
		let closer = Closer::new(1);
		let tc = closer.clone();
		std::thread::spawn(move || {
			if let Err(err) = tc.listen().recv() {
				println!("err: {}", err);
			}
			tc.done();
		});
		closer.signal_and_wait();
	}

	#[test]
	fn test_closer_many() {
		use core::time::Duration;

		use crossbeam_channel::unbounded;

		let (tx, rx) = unbounded();

		let c = Closer::default();

		for _ in 0..10 {
			let c = c.clone();
			let tx = tx.clone();
			std::thread::spawn(move || {
				assert!(c.listen().recv().is_err());
				tx.send(()).unwrap();
			});
		}
		c.signal();
		for _ in 0..10 {
			rx.recv_timeout(Duration::from_millis(10)).unwrap();
		}
	}
}
