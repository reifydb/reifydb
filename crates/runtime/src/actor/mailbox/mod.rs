// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
use std::cell::{Cell, RefCell};
use std::fmt;
#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
use std::rc::Rc;
#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
use std::sync::Arc;
#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
use std::sync::atomic::AtomicBool;

use cfg_if::cfg_if;

#[cfg(not(reifydb_single_threaded))]
pub(crate) mod host;

#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
pub(crate) mod wasm;

#[cfg(reifydb_dst)]
pub(crate) mod dst;

cfg_if! {
	if #[cfg(reifydb_dst)] {
		type ActorRefInnerImpl<M> = dst::ActorRefInner<M>;
	} else if #[cfg(not(reifydb_single_threaded))] {
		type ActorRefInnerImpl<M> = host::ActorRefInner<M>;
	} else {
		type ActorRefInnerImpl<M> = wasm::ActorRefInner<M>;
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError<M> {
	Closed(M),

	Full(M),
}

impl<M> SendError<M> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AskError {
	SendFailed,

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

// SAFETY: under reifydb_single_threaded there is no second thread, so an ActorRef can never be reached
// from another one. These impls only satisfy Send + Sync bounds and never back a real transfer.
#[cfg(reifydb_single_threaded)]
unsafe impl<M> Send for ActorRef<M> {}

#[cfg(reifydb_single_threaded)]
unsafe impl<M> Sync for ActorRef<M> {}

impl<M> ActorRef<M> {
	#[inline]
	pub(crate) fn from_inner(inner: ActorRefInnerImpl<M>) -> Self {
		Self {
			inner,
		}
	}
}

#[cfg(not(reifydb_single_threaded))]
impl<M: Send> ActorRef<M> {
	#[inline]
	pub(crate) fn new(tx: Sender<M>) -> Self {
		Self {
			inner: host::ActorRefInner::new(tx),
		}
	}

	#[inline]
	pub(crate) fn set_notify(&self, f: sync::Arc<dyn Fn() + Send + Sync>) {
		self.inner.set_notify(f)
	}

	#[inline]
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send(msg)
	}

	#[inline]
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send_blocking(msg)
	}

	#[inline]
	pub fn is_alive(&self) -> bool {
		self.inner.is_alive()
	}
}

#[cfg(reifydb_dst)]
impl<M> ActorRef<M> {
	#[inline]
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send(msg)
	}

	#[inline]
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send_blocking(msg)
	}

	#[inline]
	pub fn is_alive(&self) -> bool {
		self.inner.is_alive()
	}

	#[inline]
	pub(crate) fn mark_stopped(&self) {
		self.inner.mark_stopped()
	}

	#[inline]
	pub(crate) fn set_notify(&self, f: Box<dyn Fn()>) {
		self.inner.set_notify(f)
	}
}

#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
impl<M> ActorRef<M> {
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

	#[inline]
	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send(msg)
	}

	#[inline]
	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		self.inner.send_blocking(msg)
	}

	#[inline]
	pub fn is_alive(&self) -> bool {
		self.inner.is_alive()
	}

	#[inline]
	pub(crate) fn mark_stopped(&self) {
		self.inner.mark_stopped()
	}

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
#[cfg(reifydb_dst)]
pub(crate) use dst::create_mailbox as create_dst_mailbox;
#[cfg(not(reifydb_single_threaded))]
pub(crate) use host::create_mailbox;
#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
pub(crate) use wasm::create_actor_ref;
