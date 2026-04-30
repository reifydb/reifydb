// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cell::RefCell,
	collections::VecDeque,
	fmt,
	rc::Rc,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use super::{ActorRef, SendError};

/// DST implementation of ActorRef inner.
///
/// Messages are enqueued and a notify callback signals the actor system
/// to register the message in the global ready queue.
pub struct ActorRefInner<M> {
	/// Queue for pending messages (popped by the DST actor system during step()).
	queue: Rc<RefCell<VecDeque<M>>>,
	/// Flag indicating if the actor is still alive.
	alive: Arc<AtomicBool>,
	/// Callback that registers a new ReadyEntry in the global ready queue.
	notify: Rc<RefCell<Option<Box<dyn Fn()>>>>,
}

impl<M> Clone for ActorRefInner<M> {
	fn clone(&self) -> Self {
		Self {
			queue: self.queue.clone(),
			alive: self.alive.clone(),
			notify: self.notify.clone(),
		}
	}
}

impl<M> fmt::Debug for ActorRefInner<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorRefInner")
			.field("alive", &self.alive.load(Ordering::SeqCst))
			.field("pending", &self.queue.borrow().len())
			.finish()
	}
}

impl<M> ActorRefInner<M> {
	/// Send a message (enqueue-only in DST).
	///
	/// Pushes the message onto the queue and calls the notify callback
	/// to register a ReadyEntry in the global ready queue.
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		if !self.alive.load(Ordering::SeqCst) {
			return Err(SendError::Closed(msg));
		}

		self.queue.borrow_mut().push_back(msg);

		if let Some(ref notify) = *self.notify.borrow() {
			notify();
		}

		Ok(())
	}

	/// Send a message, blocking if the mailbox is full.
	///
	/// In DST, this is identical to `send()` since there's no backpressure.
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

	/// Install the notify callback.
	pub(crate) fn set_notify(&self, f: Box<dyn Fn()>) {
		*self.notify.borrow_mut() = Some(f);
	}
}

/// Create a DST mailbox.
///
/// Returns both the ActorRef (for sending) and the queue handle
/// (for the DST actor system to pop messages during `step()`).
pub(crate) fn create_mailbox<M>() -> (ActorRef<M>, Rc<RefCell<VecDeque<M>>>) {
	let queue = Rc::new(RefCell::new(VecDeque::new()));
	let alive = Arc::new(AtomicBool::new(true));
	let notify = Rc::new(RefCell::new(None));

	let inner = ActorRefInner {
		queue: queue.clone(),
		alive,
		notify,
	};

	(ActorRef::from_inner(inner), queue)
}
