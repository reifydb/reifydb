// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
use crate::actor::timers::drain_expired_timers;

pub struct ActorRefInner<M> {
	pub(crate) processor: Rc<RefCell<Option<Box<dyn FnMut(M)>>>>,

	pub(crate) alive: Arc<AtomicBool>,

	pub(crate) queue: Rc<RefCell<Vec<M>>>,

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

		drain_expired_timers();

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
}

pub(crate) fn create_actor_ref<M>() -> ActorRef<M> {
	ActorRef::from_inner(ActorRefInner {
		processor: Rc::new(RefCell::new(None)),
		alive: Arc::new(AtomicBool::new(true)),
		queue: Rc::new(RefCell::new(Vec::new())),
		processing: Rc::new(Cell::new(false)),
	})
}
