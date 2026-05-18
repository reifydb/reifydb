// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fmt, sync, sync::Arc, time::Duration};

use crossbeam_channel::{
	Receiver, RecvTimeoutError as CcRecvTimeoutError, SendError as CcSendError, Sender,
	TryRecvError as CcTryRecvError, TrySendError as CcTrySendError, bounded, unbounded,
};

use super::{ActorRef, RecvError, RecvTimeoutError, SendError, TryRecvError};

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

impl<M> Mailbox<M> {
	pub fn try_recv(&self) -> Result<M, TryRecvError> {
		match self.rx.try_recv() {
			Ok(msg) => Ok(msg),
			Err(CcTryRecvError::Empty) => Err(TryRecvError::Empty),
			Err(CcTryRecvError::Disconnected) => Err(TryRecvError::Closed),
		}
	}

	pub fn recv(&self) -> Result<M, RecvError> {
		match self.rx.recv() {
			Ok(msg) => Ok(msg),
			Err(_) => Err(RecvError::Closed),
		}
	}

	pub fn recv_timeout(&self, timeout: Duration) -> Result<M, RecvTimeoutError> {
		match self.rx.recv_timeout(timeout) {
			Ok(msg) => Ok(msg),
			Err(CcRecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
			Err(CcRecvTimeoutError::Disconnected) => Err(RecvTimeoutError::Closed),
		}
	}
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
