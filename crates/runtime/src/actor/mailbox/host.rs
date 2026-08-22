// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_types)]

use std::{fmt, sync, sync::Arc};

use crossbeam_channel::{
	Receiver, SendError as CcSendError, Sender, TrySendError as CcTrySendError, bounded, unbounded,
};

use super::{ActorRef, SendError};

pub struct ActorRefInner<M> {
	pub(crate) tx: Sender<M>,
	notify: Arc<sync::OnceLock<Arc<dyn Fn() + Send + Sync>>>,
}

impl<M> Clone for ActorRefInner<M> {
	fn clone(&self) -> Self {
		Self {
			tx: self.tx.clone(),
			notify: Arc::clone(&self.notify),
		}
	}
}

impl<M> fmt::Debug for ActorRefInner<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorRefInner").field("capacity", &self.tx.capacity()).finish()
	}
}

impl<M: Send> ActorRefInner<M> {
	pub(crate) fn new(tx: Sender<M>) -> Self {
		Self {
			tx,
			notify: Arc::new(sync::OnceLock::new()),
		}
	}

	pub(crate) fn set_notify(&self, f: Arc<dyn Fn() + Send + Sync>) {
		let _ = self.notify.set(f);
	}

	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.try_send(msg) {
			Ok(()) => {
				if let Some(f) = self.notify.get() {
					f();
				}
				Ok(())
			}
			Err(CcTrySendError::Disconnected(m)) => Err(SendError::Closed(m)),
			Err(CcTrySendError::Full(m)) => Err(SendError::Full(m)),
		}
	}

	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.send(msg) {
			Ok(()) => {
				if let Some(f) = self.notify.get() {
					f();
				}
				Ok(())
			}
			Err(CcSendError(m)) => Err(SendError::Closed(m)),
		}
	}

	pub fn is_alive(&self) -> bool {
		!self.tx.is_empty() || self.tx.capacity().is_some()
	}
}

pub(crate) struct Mailbox<M> {
	pub(crate) rx: Receiver<M>,
}

pub(crate) fn create_mailbox<M: Send>(capacity: Option<usize>) -> (ActorRef<M>, Mailbox<M>) {
	let (tx, rx) = match capacity {
		None => unbounded(),
		Some(n) => bounded(n),
	};

	(
		ActorRef::from_inner(ActorRefInner::new(tx)),
		Mailbox {
			rx,
		},
	)
}
