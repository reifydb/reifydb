// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor mailbox and message sending types.
//!
//! This module provides:
//! - [`ActorRef`]: A handle for sending messages to an actor
//! - [`SendError`]: Error type for failed sends
//!
//! # Platform Differences
//!
//! - **Native**: Uses `crossbeam-channel` for lock-free message passing between threads
//! - **WASM**: Uses `Rc<RefCell>` processor for inline (synchronous) message handling

use std::fmt;

/// Error returned when sending a message fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError<M> {
	/// The actor has stopped and the mailbox is closed.
	Closed(M),
	/// The mailbox is full (bounded mailbox only).
	Full(M),
}

impl<M> SendError<M> {
	/// Get the message that failed to send.
	pub fn into_inner(self) -> M {
		match self {
			SendError::Closed(m) => m,
			SendError::Full(m) => m,
		}
	}
}

impl<M: fmt::Debug> fmt::Display for SendError<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SendError::Closed(_) => write!(f, "actor mailbox closed"),
			SendError::Full(_) => write!(f, "actor mailbox full"),
		}
	}
}

impl<M: fmt::Debug> std::error::Error for SendError<M> {}

/// Error returned when an ask (request-response) fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AskError {
	/// Failed to send the request.
	SendFailed,
	/// The response channel was closed (actor stopped or didn't respond).
	ResponseClosed,
}

impl fmt::Display for AskError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			AskError::SendFailed => write!(f, "failed to send ask request"),
			AskError::ResponseClosed => write!(f, "response channel closed"),
		}
	}
}

impl std::error::Error for AskError {}

/// Error when trying to receive without blocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
	/// No message available.
	Empty,
	/// Mailbox closed.
	Closed,
}

/// Error when receiving blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
	/// Mailbox closed.
	Closed,
}

/// Error when receiving with timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimeoutError {
	/// Timeout elapsed without receiving a message.
	Timeout,
	/// Mailbox closed.
	Closed,
}

// =============================================================================
// Native: crossbeam-channel based implementation
// =============================================================================

/// Handle to send messages to an actor.
///
/// Uses `crossbeam-channel` for lock-free message passing.
/// Cheap to clone, safe to share across threads.
#[cfg(feature = "native")]
pub struct ActorRef<M> {
	pub(crate) tx: crossbeam_channel::Sender<M>,
}

#[cfg(feature = "native")]
impl<M> Clone for ActorRef<M> {
	fn clone(&self) -> Self {
		Self { tx: self.tx.clone() }
	}
}

#[cfg(feature = "native")]
impl<M> fmt::Debug for ActorRef<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorRef")
			.field("capacity", &self.tx.capacity())
			.finish()
	}
}

#[cfg(feature = "native")]
impl<M: Send> ActorRef<M> {
	/// Create a new ActorRef from a sender.
	pub(crate) fn new(tx: crossbeam_channel::Sender<M>) -> Self {
		Self { tx }
	}

	/// Send a message (non-blocking, may fail if mailbox full).
	///
	/// Returns `Ok(())` if the message was queued successfully.
	/// Returns `Err(SendError::Closed)` if the actor has stopped.
	/// Returns `Err(SendError::Full)` if the mailbox is full (bounded only).
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.try_send(msg) {
			Ok(()) => Ok(()),
			Err(crossbeam_channel::TrySendError::Disconnected(m)) => Err(SendError::Closed(m)),
			Err(crossbeam_channel::TrySendError::Full(m)) => Err(SendError::Full(m)),
		}
	}

	/// Send a message, blocking if the mailbox is full.
	///
	/// This is useful when you want backpressure - the sender will
	/// block until there's room in the mailbox.
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.send(msg) {
			Ok(()) => Ok(()),
			Err(crossbeam_channel::SendError(m)) => Err(SendError::Closed(m)),
		}
	}

	/// Check if the actor is still alive.
	///
	/// Returns `false` if the actor has stopped and the mailbox is closed.
	pub fn is_alive(&self) -> bool {
		!self.tx.is_empty() || self.tx.capacity().is_some()
	}
}

/// Internal receiver for the actor's mailbox.
#[cfg(feature = "native")]
pub(crate) struct Mailbox<M> {
	pub(crate) rx: crossbeam_channel::Receiver<M>,
}

#[cfg(feature = "native")]
impl<M> Mailbox<M> {
	/// Try to receive a message without blocking.
	pub fn try_recv(&self) -> Result<M, TryRecvError> {
		match self.rx.try_recv() {
			Ok(msg) => Ok(msg),
			Err(crossbeam_channel::TryRecvError::Empty) => Err(TryRecvError::Empty),
			Err(crossbeam_channel::TryRecvError::Disconnected) => Err(TryRecvError::Closed),
		}
	}

	/// Receive a message, blocking if necessary.
	pub fn recv(&self) -> Result<M, RecvError> {
		match self.rx.recv() {
			Ok(msg) => Ok(msg),
			Err(_) => Err(RecvError::Closed),
		}
	}

	/// Receive a message with a timeout.
	///
	/// Returns `Ok(msg)` if a message is received within the timeout.
	/// Returns `Err(RecvTimeoutError::Timeout)` if the timeout elapsed.
	/// Returns `Err(RecvTimeoutError::Closed)` if the mailbox is closed.
	pub fn recv_timeout(&self, timeout: std::time::Duration) -> Result<M, RecvTimeoutError> {
		match self.rx.recv_timeout(timeout) {
			Ok(msg) => Ok(msg),
			Err(crossbeam_channel::RecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
			Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Err(RecvTimeoutError::Closed),
		}
	}
}

/// Create a mailbox channel pair with the given capacity.
///
/// If capacity is 0, creates an unbounded channel.
#[cfg(feature = "native")]
pub(crate) fn create_mailbox<M: Send>(capacity: usize) -> (ActorRef<M>, Mailbox<M>) {
	let (tx, rx) = if capacity == 0 {
		crossbeam_channel::unbounded()
	} else {
		crossbeam_channel::bounded(capacity)
	};

	(ActorRef::new(tx), Mailbox { rx })
}

// =============================================================================
// WASM: Inline processor based implementation
// =============================================================================

/// Handle to send messages to an actor.
///
/// In WASM, messages are processed synchronously inline when sent.
/// This is because WASM runs single-threaded.
#[cfg(feature = "wasm")]
pub struct ActorRef<M> {
	/// The processor function that handles messages inline.
	pub(crate) processor: std::rc::Rc<std::cell::RefCell<Option<Box<dyn FnMut(M)>>>>,
	/// Flag indicating if the actor is still alive.
	pub(crate) alive: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "wasm")]
impl<M> Clone for ActorRef<M> {
	fn clone(&self) -> Self {
		Self {
			processor: self.processor.clone(),
			alive: self.alive.clone(),
		}
	}
}

#[cfg(feature = "wasm")]
impl<M> fmt::Debug for ActorRef<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		use std::sync::atomic::Ordering;
		f.debug_struct("ActorRef")
			.field("alive", &self.alive.load(Ordering::SeqCst))
			.finish()
	}
}

#[cfg(feature = "wasm")]
impl<M> ActorRef<M> {
	/// Create a new ActorRef for WASM with a processor function.
	pub(crate) fn new(
		processor: std::rc::Rc<std::cell::RefCell<Option<Box<dyn FnMut(M)>>>>,
		alive: std::sync::Arc<std::sync::atomic::AtomicBool>,
	) -> Self {
		Self { processor, alive }
	}

	/// Send a message (processes synchronously inline in WASM).
	///
	/// Returns `Ok(())` if the message was processed successfully.
	/// Returns `Err(SendError::Closed)` if the actor has stopped.
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		use std::sync::atomic::Ordering;

		if !self.alive.load(Ordering::SeqCst) {
			return Err(SendError::Closed(msg));
		}

		let mut processor_ref = self.processor.borrow_mut();
		if let Some(ref mut processor) = *processor_ref {
			processor(msg);
			Ok(())
		} else {
			Err(SendError::Closed(msg))
		}
	}

	/// Send a message, blocking if the mailbox is full.
	///
	/// In WASM, this is identical to `send()` since processing is inline.
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.send(msg)
	}

	/// Check if the actor is still alive.
	pub fn is_alive(&self) -> bool {
		use std::sync::atomic::Ordering;
		self.alive.load(Ordering::SeqCst)
	}

	/// Mark the actor as stopped.
	pub(crate) fn mark_stopped(&self) {
		use std::sync::atomic::Ordering;
		self.alive.store(false, Ordering::SeqCst);
	}
}

/// Create an ActorRef for WASM (no mailbox needed since processing is inline).
#[cfg(feature = "wasm")]
pub(crate) fn create_actor_ref<M>() -> ActorRef<M> {
	use std::cell::RefCell;
	use std::rc::Rc;
	use std::sync::atomic::AtomicBool;
	use std::sync::Arc;

	ActorRef {
		processor: Rc::new(RefCell::new(None)),
		alive: Arc::new(AtomicBool::new(true)),
	}
}
