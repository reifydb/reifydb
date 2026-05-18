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

pub struct ActorRefInner<M> {
	queue: Rc<RefCell<VecDeque<M>>>,

	alive: Arc<AtomicBool>,

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

	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.send(msg)
	}

	pub fn is_alive(&self) -> bool {
		self.alive.load(Ordering::SeqCst)
	}

	pub(crate) fn mark_stopped(&self) {
		self.alive.store(false, Ordering::SeqCst);
	}

	pub(crate) fn set_notify(&self, f: Box<dyn Fn()>) {
		*self.notify.borrow_mut() = Some(f);
	}
}

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
