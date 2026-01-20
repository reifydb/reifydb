// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native worker thread implementation using OS threads.

use crossbeam_channel::{unbounded, Receiver, RecvError, RecvTimeoutError, SendError, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Error type for sending messages to a worker thread.
pub type WorkerSendError<M> = SendError<M>;

/// A worker thread that processes messages in the background.
///
/// Native implementation spawns an OS thread with a crossbeam channel.
pub struct WorkerThread<M> {
	sender: Sender<M>,
	handle: Option<JoinHandle<()>>,
}

impl<M: Send + 'static> WorkerThread<M> {
	/// Spawn a new worker thread with the given name and worker function.
	///
	/// The worker function receives a `WorkerReceiver<M>` and should loop
	/// receiving messages until the channel is closed (sender dropped).
	///
	/// # Panics
	///
	/// Panics if the thread cannot be spawned.
	pub fn spawn<F>(name: String, worker_fn: F) -> Self
	where
		F: FnOnce(WorkerReceiver<M>) + Send + 'static,
	{
		let (tx, rx) = unbounded();
		let receiver = WorkerReceiver { inner: rx };

		let handle = thread::Builder::new()
			.name(name)
			.spawn(move || worker_fn(receiver))
			.expect("Failed to spawn worker thread");

		Self { sender: tx, handle: Some(handle) }
	}

	/// Send a message to the worker thread.
	///
	/// Returns an error if the worker thread has panicked or been stopped.
	pub fn send(&self, msg: M) -> Result<(), WorkerSendError<M>> {
		self.sender.send(msg)
	}

	/// Stop the worker thread and wait for it to finish.
	///
	/// Drops the sender to signal the worker to stop, then joins the thread.
	/// This consumes the WorkerThread, and cleanup happens via the Drop impl.
	pub fn stop(self) {
		// Drop implementation will handle cleanup automatically
		// Sender is dropped, signaling worker to stop
		// Thread handle is joined in Drop
	}
}

impl<M> Drop for WorkerThread<M> {
	fn drop(&mut self) {
		// Sender is automatically dropped here, signaling the worker to stop
		// Join the thread if handle is still present
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

/// Receiver for messages sent to a worker thread.
pub struct WorkerReceiver<M> {
	inner: Receiver<M>,
}

impl<M> WorkerReceiver<M> {
	/// Receive a message, blocking until one is available.
	///
	/// Returns an error if the sender has been dropped (channel closed).
	pub fn recv(&self) -> Result<M, RecvError> {
		self.inner.recv()
	}

	/// Receive a message with a timeout.
	///
	/// Returns an error if the timeout expires or the channel is closed.
	pub fn recv_timeout(&self, timeout: Duration) -> Result<M, RecvTimeoutError> {
		self.inner.recv_timeout(timeout)
	}
}
