// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM mailbox implementation using inline processing.
//!
//! In WASM, messages are processed synchronously inline when sent.
//! Uses a message queue to handle reentrancy (when message handling triggers more messages).

use std::{
	cell::{Cell, RefCell},
	fmt,
	rc::Rc,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use super::{ActorRef, SendError};

/// WASM implementation of ActorRef inner.
///
/// Messages are processed synchronously inline when sent.
/// Uses a message queue to handle reentrancy.
pub struct ActorRefInner<M> {
	/// The processor function that handles messages inline.
	pub(crate) processor: Rc<RefCell<Option<Box<dyn FnMut(M)>>>>,
	/// Flag indicating if the actor is still alive.
	pub(crate) alive: Arc<AtomicBool>,
	/// Queue for messages that arrive during processing (handles reentrancy).
	pub(crate) queue: Rc<RefCell<Vec<M>>>,
	/// Flag to track if we're currently processing (prevents reentrant processing).
	pub(crate) processing: Rc<Cell<bool>>,
}

impl<M> Clone for ActorRefInner<M> {
	fn clone(&self) -> Self {
		Self {
			processor: self.processor.clone(),
			alive: self.alive.clone(),
			queue: self.queue.clone(),
			processing: self.processing.clone(),
		}
	}
}

impl<M> fmt::Debug for ActorRefInner<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorRefInner").field("alive", &self.alive.load(Ordering::SeqCst)).finish()
	}
}

impl<M> ActorRefInner<M> {
	/// Create a new ActorRefInner for WASM.
	pub(crate) fn new(
		processor: Rc<RefCell<Option<Box<dyn FnMut(M)>>>>,
		alive: Arc<AtomicBool>,
		queue: Rc<RefCell<Vec<M>>>,
		processing: Rc<Cell<bool>>,
	) -> Self {
		Self {
			processor,
			alive,
			queue,
			processing,
		}
	}

	/// Send a message (processes synchronously inline in WASM).
	///
	/// If we're already processing a message (reentrant call), the message
	/// is queued and will be processed after the current message completes.
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		if !self.alive.load(Ordering::SeqCst) {
			return Err(SendError::Closed(msg));
		}

		if self.processing.get() {
			self.queue.borrow_mut().push(msg);
			return Ok(());
		}

		self.processing.set(true);

		{
			let mut processor_ref = self.processor.borrow_mut();
			if let Some(ref mut processor) = *processor_ref {
				processor(msg);
			} else {
				self.processing.set(false);
				return Err(SendError::Closed(msg));
			}
		}

		loop {
			let next_msg = self.queue.borrow_mut().pop();
			match next_msg {
				Some(queued_msg) => {
					let mut processor_ref = self.processor.borrow_mut();
					if let Some(ref mut processor) = *processor_ref {
						processor(queued_msg);
					}
				}
				None => break,
			}
		}

		self.processing.set(false);
		Ok(())
	}

	/// Send a message, blocking if the mailbox is full.
	///
	/// In WASM, this is identical to `send()` since processing is inline.
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.send(msg)
	}

	/// Check if the actor is still alive.
	pub fn is_alive(&self) -> bool {
		self.alive.load(Ordering::SeqCst)
	}

	/// Mark the actor as stopped.
	pub(crate) fn mark_stopped(&self) {
		self.alive.store(false, Ordering::SeqCst);
	}
}

/// Create an ActorRef for WASM.
pub(crate) fn create_actor_ref<M>() -> ActorRef<M> {
	ActorRef::from_inner(ActorRefInner {
		processor: Rc::new(RefCell::new(None)),
		alive: Arc::new(AtomicBool::new(true)),
		queue: Rc::new(RefCell::new(Vec::new())),
		processing: Rc::new(Cell::new(false)),
	})
}
