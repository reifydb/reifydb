// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::WaitGroup;
use tokio::sync::broadcast;

/// Closer holds the two things we need to close a task and wait for it to
/// finish: a channel to tell the task to shut down, and a WaitGroup with
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
	tx: broadcast::Sender<()>,
	signaled: AtomicBool,
}

impl CloserInner {
	fn new() -> Self {
		let (tx, _rx) = broadcast::channel(1);
		Self {
			wg: WaitGroup::new(),
			tx,
			signaled: AtomicBool::new(false),
		}
	}

	fn with(initial: usize) -> Self {
		let (tx, _rx) = broadcast::channel(1);
		Self {
			wg: WaitGroup::from(initial),
			tx,
			signaled: AtomicBool::new(false),
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

	/// Signals the shutdown.
	pub fn signal(&self) {
		// Only signal once
		if !self.signaled.swap(true, Ordering::AcqRel) {
			let _ = self.tx.send(());
		}
	}

	/// Waits on the [`WaitGroup`]. (It waits for the Closer's initial
	/// value, [`Closer::add_running`], and [`Closer::done`]
	pub async fn wait(&self) {
		self.wg.wait().await;
	}

	/// Calls [`Closer::signal`], then [`Closer::wait`].
	pub async fn signal_and_wait(&self) {
		self.signal();
		self.wait().await;
	}

	/// Listens for the shutdown signal.
	/// Returns a receiver that will receive a message when signal() is called.
	pub fn listen(&self) -> broadcast::Receiver<()> {
		self.tx.subscribe()
	}

	/// Returns true if signal() has been called.
	pub fn is_signaled(&self) -> bool {
		self.signaled.load(Ordering::Acquire)
	}
}

#[cfg(test)]
mod tests {
	use tokio::{
		spawn,
		time::{Duration, sleep, timeout},
	};

	use crate::multi::watermark::Closer;

	#[tokio::test]
	async fn test_multiple_singles() {
		let closer = Closer::default();
		closer.signal();
		closer.signal();
		closer.signal_and_wait().await;

		let closer = Closer::new(1);
		closer.done();
		closer.signal_and_wait().await;
		closer.signal_and_wait().await;
		closer.signal();
	}

	#[tokio::test]
	async fn test_closer_single() {
		let closer = Closer::new(1);
		let tc = closer.clone();
		spawn(async move {
			let mut rx = tc.listen();
			let _ = rx.recv().await;
			tc.done();
		});
		// Give tasks time to start and subscribe
		sleep(Duration::from_millis(10)).await;
		closer.signal_and_wait().await;
	}

	#[tokio::test]
	async fn test_closer_many() {
		use tokio::sync::mpsc;

		let (tx, mut rx) = mpsc::channel(10);

		let c = Closer::default();

		for _ in 0..10 {
			let c = c.clone();
			let tx = tx.clone();
			spawn(async move {
				let mut listener = c.listen();
				// Wait for signal (recv returns Err when sender is dropped or message received)
				let _ = listener.recv().await;
				tx.send(()).await.unwrap();
			});
		}

		// Give tasks time to start and subscribe
		sleep(Duration::from_millis(10)).await;

		c.signal();

		for _ in 0..10 {
			timeout(Duration::from_millis(100), rx.recv()).await.expect("timeout").expect("channel closed");
		}
	}
}
