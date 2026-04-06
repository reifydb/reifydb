// SPDX-License-Identifier: Apache-2.0
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

#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
use std::cell::{Cell, RefCell};
use std::fmt;
#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
use std::rc::Rc;
#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
use std::sync::Arc;
#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
use std::sync::atomic::AtomicBool;

use cfg_if::cfg_if;

#[cfg(not(reifydb_single_threaded))]
pub(crate) mod native;

#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
pub(crate) mod wasm;

#[cfg(reifydb_target = "dst")]
pub(crate) mod dst;

cfg_if! {
	if #[cfg(reifydb_target = "dst")] {
		type ActorRefInnerImpl<M> = dst::ActorRefInner<M>;
	} else if #[cfg(not(reifydb_single_threaded))] {
		type ActorRefInnerImpl<M> = native::ActorRefInner<M>;
	} else {
		type ActorRefInnerImpl<M> = wasm::ActorRefInner<M>;
	}
}

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
	#[inline]
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

impl<M: fmt::Debug> error::Error for SendError<M> {}

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

impl error::Error for AskError {}

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

/// Handle to send messages to an actor.
///
/// - **Native**: Uses `crossbeam-channel` for lock-free message passing
/// - **WASM**: Messages are processed synchronously inline when sent
///
/// Cheap to clone, safe to share across threads (native) or within single thread (WASM).
pub struct ActorRef<M> {
	inner: ActorRefInnerImpl<M>,
}

impl<M> Clone for ActorRef<M> {
	#[inline]
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl<M> fmt::Debug for ActorRef<M> {
	#[inline]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.inner.fmt(f)
	}
}

// SAFETY: Single-threaded targets (WASM/WASI) don't have real concurrency
#[cfg(reifydb_single_threaded)]
unsafe impl<M> Send for ActorRef<M> {}

#[cfg(reifydb_single_threaded)]
unsafe impl<M> Sync for ActorRef<M> {}

impl<M> ActorRef<M> {
	/// Create a new ActorRef from an inner implementation.
	#[inline]
	pub(crate) fn from_inner(inner: ActorRefInnerImpl<M>) -> Self {
		Self {
			inner,
		}
	}
}

// Native-specific methods (require M: Send)
#[cfg(not(reifydb_single_threaded))]
impl<M: Send> ActorRef<M> {
	/// Create a new ActorRef from a crossbeam sender (native only).
	#[inline]
	pub(crate) fn new(tx: Sender<M>) -> Self {
		Self {
			inner: native::ActorRefInner::new(tx),
		}
	}

	/// Set the notify callback, called on successful send to wake the actor.
	#[inline]
	pub(crate) fn set_notify(&self, f: sync::Arc<dyn Fn() + Send + Sync>) {
		self.inner.set_notify(f)
	}

	/// Send a message (non-blocking, may fail if mailbox full).
	///
	/// Returns `Ok(())` if the message was queued/processed successfully.
	/// Returns `Err(SendError::Closed)` if the actor has stopped.
	/// Returns `Err(SendError::Full)` if the mailbox is full (bounded only).
	#[inline]
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send(msg)
	}

	/// Send a message, blocking if the mailbox is full.
	///
	/// This provides backpressure - sender blocks until there's room.
	#[inline]
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send_blocking(msg)
	}

	/// Check if the actor is still alive.
	///
	/// Returns `false` if the actor has stopped and the mailbox is closed.
	#[inline]
	pub fn is_alive(&self) -> bool {
		self.inner.is_alive()
	}
}

// DST-specific methods (no Send bound needed)
#[cfg(reifydb_target = "dst")]
impl<M> ActorRef<M> {
	/// Send a message (enqueue-only in DST).
	#[inline]
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send(msg)
	}

	/// Send a message, blocking if the mailbox is full.
	///
	/// In DST, this is identical to `send()`.
	#[inline]
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send_blocking(msg)
	}

	/// Check if the actor is still alive.
	#[inline]
	pub fn is_alive(&self) -> bool {
		self.inner.is_alive()
	}

	/// Mark the actor as stopped (DST only).
	#[inline]
	pub(crate) fn mark_stopped(&self) {
		self.inner.mark_stopped()
	}

	/// Install the notify callback (DST only).
	#[inline]
	pub(crate) fn set_notify(&self, f: Box<dyn Fn()>) {
		self.inner.set_notify(f)
	}
}

// Single-threaded methods (no Send bound needed)
#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
impl<M> ActorRef<M> {
	/// Create a new ActorRef with WASM components (WASM only).
	#[inline]
	pub(crate) fn new(
		processor: Rc<RefCell<Option<Box<dyn FnMut(M)>>>>,
		alive: Arc<AtomicBool>,
		queue: Rc<RefCell<Vec<M>>>,
		processing: Rc<Cell<bool>>,
	) -> Self {
		Self {
			inner: wasm::ActorRefInner::new(processor, alive, queue, processing),
		}
	}

	/// Create a new ActorRef from WASM inner components (used by system/wasm).
	#[inline]
	pub(crate) fn from_wasm_inner(
		processor: Rc<RefCell<Option<Box<dyn FnMut(M)>>>>,
		alive: Arc<AtomicBool>,
		queue: Rc<RefCell<Vec<M>>>,
		processing: Rc<Cell<bool>>,
	) -> Self {
		Self {
			inner: wasm::ActorRefInner::new(processor, alive, queue, processing),
		}
	}

	/// Send a message (processes synchronously inline in WASM).
	///
	/// Returns `Ok(())` if the message was processed/queued successfully.
	/// Returns `Err(SendError::Closed)` if the actor has stopped.
	#[inline]
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send(msg)
	}

	/// Send a message, blocking if the mailbox is full.
	///
	/// In WASM, this is identical to `send()` since processing is inline.
	#[inline]
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send_blocking(msg)
	}

	/// Check if the actor is still alive.
	///
	/// Returns `false` if the actor has stopped.
	#[inline]
	pub fn is_alive(&self) -> bool {
		self.inner.is_alive()
	}

	/// Mark the actor as stopped (WASM only).
	#[inline]
	pub(crate) fn mark_stopped(&self) {
		self.inner.mark_stopped()
	}

	/// Get access to the processor for setting it up (WASM only).
	#[inline]
	pub(crate) fn processor(&self) -> &Rc<RefCell<Option<Box<dyn FnMut(M)>>>> {
		&self.inner.processor
	}
}

use std::error;
#[cfg(not(reifydb_single_threaded))]
use std::sync;

#[cfg(not(reifydb_single_threaded))]
use crossbeam_channel::Sender;
#[cfg(reifydb_target = "dst")]
pub(crate) use dst::create_mailbox as create_dst_mailbox;
#[cfg(not(reifydb_single_threaded))]
pub(crate) use native::create_mailbox;
#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
pub(crate) use wasm::create_actor_ref;
