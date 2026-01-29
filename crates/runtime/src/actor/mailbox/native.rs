// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native mailbox implementation using crossbeam-channel.

use std::{fmt, time::Duration};

use crossbeam_channel::Receiver;

use super::{ActorRef, RecvError, RecvTimeoutError, SendError, TryRecvError};

/// Native implementation of ActorRef inner.
///
/// Uses `crossbeam-channel` for lock-free message passing.
pub struct ActorRefInner<M> {
	pub(crate) tx: crossbeam_channel::Sender<M>,
}

impl<M> Clone for ActorRefInner<M> {
	fn clone(&self) -> Self {
		Self {
			tx: self.tx.clone(),
		}
	}
}

impl<M> fmt::Debug for ActorRefInner<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorRefInner").field("capacity", &self.tx.capacity()).finish()
	}
}

impl<M: Send> ActorRefInner<M> {
	/// Create a new ActorRefInner from a sender.
	pub(crate) fn new(tx: crossbeam_channel::Sender<M>) -> Self {
		Self {
			tx,
		}
	}

	/// Send a message (non-blocking, may fail if mailbox full).
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.try_send(msg) {
			Ok(()) => Ok(()),
			Err(crossbeam_channel::TrySendError::Disconnected(m)) => Err(SendError::Closed(m)),
			Err(crossbeam_channel::TrySendError::Full(m)) => Err(SendError::Full(m)),
		}
	}

	/// Send a message, blocking if the mailbox is full.
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.send(msg) {
			Ok(()) => Ok(()),
			Err(crossbeam_channel::SendError(m)) => Err(SendError::Closed(m)),
		}
	}

	/// Check if the actor is still alive.
	pub fn is_alive(&self) -> bool {
		!self.tx.is_empty() || self.tx.capacity().is_some()
	}
}

/// Internal receiver for the actor's mailbox.
pub(crate) struct Mailbox<M> {
	pub(crate) rx: Receiver<M>,
}

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
	pub fn recv_timeout(&self, timeout: Duration) -> Result<M, RecvTimeoutError> {
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
pub(crate) fn create_mailbox<M: Send>(capacity: usize) -> (ActorRef<M>, Mailbox<M>) {
	let (tx, rx) = if capacity == 0 {
		crossbeam_channel::unbounded()
	} else {
		crossbeam_channel::bounded(capacity)
	};

	(
		ActorRef::from_inner(ActorRefInner::new(tx)),
		Mailbox {
			rx,
		},
	)
}
