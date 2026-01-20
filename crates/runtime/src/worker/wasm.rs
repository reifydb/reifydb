// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM worker thread implementation using synchronous processing.

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

/// Error type for sending messages to a worker thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSendError<M> {
	_phantom: PhantomData<M>,
}

impl<M> fmt::Display for WorkerSendError<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "failed to send message to worker")
	}
}

impl<M: fmt::Debug> std::error::Error for WorkerSendError<M> {}

/// A worker thread that processes messages synchronously.
///
/// WASM implementation processes messages immediately on `send()`.
/// The worker function is called once during construction but doesn't
/// actually receive messages through the receiver.
pub struct WorkerThread<M> {
	processor: Option<Rc<RefCell<Box<dyn FnMut(M)>>>>,
}

impl<M: 'static> WorkerThread<M> {
	/// Create a new worker thread with the given name and worker function.
	///
	/// In WASM, the worker function is provided for API compatibility but
	/// messages are processed synchronously on `send()` instead.
	///
	/// For WASM builds, it's recommended to use platform-specific code
	/// that directly processes messages instead of spawning a worker.
	pub fn spawn<F>(_name: String, _worker_fn: F) -> Self
	where
		F: FnOnce(WorkerReceiver<M>) + 'static,
	{
		// In WASM, we don't actually spawn a thread or run the worker_fn
		// Messages will be processed synchronously on send()
		// The caller should use conditional compilation for WASM-specific behavior
		Self { processor: None }
	}

	/// Create a worker with a direct processor function (WASM-specific).
	///
	/// This method is specific to WASM and allows providing a processor
	/// function that will be called synchronously for each message.
	pub fn with_processor<F>(processor: F) -> Self
	where
		F: FnMut(M) + 'static,
	{
		Self { processor: Some(Rc::new(RefCell::new(Box::new(processor)))) }
	}

	/// Send a message to the worker thread.
	///
	/// In WASM, this processes the message synchronously if a processor
	/// is available.
	pub fn send(&self, msg: M) -> Result<(), WorkerSendError<M>> {
		if let Some(ref processor) = self.processor {
			let mut proc = processor.borrow_mut();
			proc(msg);
			Ok(())
		} else {
			// No processor - message is dropped
			// This happens when using spawn() instead of with_processor()
			Ok(())
		}
	}

	/// Stop the worker thread.
	///
	/// In WASM, this is a no-op.
	pub fn stop(self) {
		// No-op in WASM
	}
}

/// Receiver for messages sent to a worker thread.
///
/// In WASM, this exists for API compatibility but doesn't actually
/// receive messages since processing is synchronous.
pub struct WorkerReceiver<M> {
	_phantom: PhantomData<M>,
}

impl<M> WorkerReceiver<M> {
	/// Create a new receiver (WASM-specific, for compatibility).
	#[allow(dead_code)]
	pub(crate) fn new() -> Self {
		Self { _phantom: PhantomData }
	}

	/// Receive a message, blocking until one is available.
	///
	/// **Not supported in WASM** - will panic if called.
	/// Messages are processed synchronously on `send()` instead.
	pub fn recv(&self) -> Result<M, RecvError> {
		panic!("WorkerReceiver::recv() is not supported in WASM - messages are processed synchronously")
	}

	/// Receive a message with a timeout.
	///
	/// **Not supported in WASM** - will panic if called.
	/// Messages are processed synchronously on `send()` instead.
	pub fn recv_timeout(&self, _timeout: std::time::Duration) -> Result<M, RecvTimeoutError> {
		panic!("WorkerReceiver::recv_timeout() is not supported in WASM - messages are processed synchronously")
	}
}

/// Error returned from `recv()` when the channel is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "channel closed")
	}
}

impl std::error::Error for RecvError {}

/// Error returned from `recv_timeout()` when the timeout expires or channel is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimeoutError {
	Timeout,
	Disconnected,
}

impl fmt::Display for RecvTimeoutError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			RecvTimeoutError::Timeout => write!(f, "timed out waiting for message"),
			RecvTimeoutError::Disconnected => write!(f, "channel closed"),
		}
	}
}

impl std::error::Error for RecvTimeoutError {}
