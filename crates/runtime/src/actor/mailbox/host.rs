// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_types)]

use std::{fmt, sync, sync::Arc};

use crossbeam_channel::{
	Receiver, SendError as CcSendError, Sender, TrySendError as CcTrySendError, bounded, unbounded,
};

use super::{ActorRef, SendError};
use crate::context::clock::{Clock, Instant};

pub struct ActorRefInner<M> {
	pub(crate) tx: Sender<(Instant, M)>,
	notify: Arc<sync::OnceLock<Arc<dyn Fn() + Send + Sync>>>,
	clock: Clock,
}

impl<M> Clone for ActorRefInner<M> {
	fn clone(&self) -> Self {
		Self {
			tx: self.tx.clone(),
			notify: Arc::clone(&self.notify),
			clock: self.clock.clone(),
		}
	}
}

impl<M> fmt::Debug for ActorRefInner<M> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorRefInner").field("capacity", &self.tx.capacity()).finish()
	}
}

impl<M: Send> ActorRefInner<M> {
	pub(crate) fn new(tx: Sender<(Instant, M)>, clock: Clock) -> Self {
		Self {
			tx,
			notify: Arc::new(sync::OnceLock::new()),
			clock,
		}
	}

	pub(crate) fn set_notify(&self, f: Arc<dyn Fn() + Send + Sync>) {
		let _ = self.notify.set(f);
	}

	pub fn send(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.try_send((self.clock.instant(), msg)) {
			Ok(()) => {
				if let Some(f) = self.notify.get() {
					f();
				}
				Ok(())
			}
			Err(CcTrySendError::Disconnected((_, m))) => Err(SendError::Closed(m)),
			Err(CcTrySendError::Full((_, m))) => Err(SendError::Full(m)),
		}
	}

	pub fn send_blocking(&self, msg: M) -> Result<(), SendError<M>> {
		match self.tx.send((self.clock.instant(), msg)) {
			Ok(()) => {
				if let Some(f) = self.notify.get() {
					f();
				}
				Ok(())
			}
			Err(CcSendError((_, m))) => Err(SendError::Closed(m)),
		}
	}

	pub fn is_alive(&self) -> bool {
		!self.tx.is_empty() || self.tx.capacity().is_some()
	}
}

pub(crate) struct Mailbox<M> {
	pub(crate) rx: Receiver<(Instant, M)>,
	pub(crate) tx: Sender<(Instant, M)>,
}

pub(crate) fn create_mailbox<M: Send>(capacity: Option<usize>, clock: Clock) -> (ActorRef<M>, Mailbox<M>) {
	let (tx, rx) = match capacity {
		None => unbounded(),
		Some(n) => bounded(n),
	};

	(
		ActorRef::from_inner(ActorRefInner::new(tx.clone(), clock)),
		Mailbox {
			rx,
			tx,
		},
	)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use crate::context::clock::MockClock;

	#[test]
	fn a_message_carries_the_clock_instant_of_its_send() {
		// A stamp taken at receive would report every mailbox wait as zero.
		let mock = MockClock::new(1_000);
		let clock = Clock::Mock(mock.clone());
		let (actor, mailbox) = create_mailbox::<u8>(None, clock.clone());

		assert!(actor.send(7).is_ok(), "precondition: an open mailbox takes the message");
		mock.advance_millis(250);

		let (sent_at, message) = mailbox.rx.try_recv().expect("the sent message must be in the mailbox");
		assert_eq!(message, 7);
		assert_eq!(clock.instant().duration_since(&sent_at), Duration::from_millis(250));
	}

	#[test]
	fn a_blocking_send_carries_the_clock_instant_of_its_send() {
		// The blocking path must stamp too, otherwise its messages wait unseen.
		let mock = MockClock::new(1_000);
		let clock = Clock::Mock(mock.clone());
		let (actor, mailbox) = create_mailbox::<u8>(None, clock.clone());

		assert!(actor.send_blocking(7).is_ok(), "precondition: an open mailbox takes the message");
		mock.advance_millis(40);

		let (sent_at, _) = mailbox.rx.try_recv().expect("the sent message must be in the mailbox");
		assert_eq!(clock.instant().duration_since(&sent_at), Duration::from_millis(40));
	}

	#[test]
	fn a_refused_send_hands_back_the_message_without_its_stamp() {
		// The caller must get its own message back, otherwise a retry would send a stamped pair.
		let (actor, mailbox) = create_mailbox::<u8>(Some(1), Clock::Real);
		assert!(actor.send(1).is_ok(), "precondition: the first send fits");

		assert!(matches!(actor.send(2), Err(SendError::Full(2))));
		drop(mailbox);
		assert!(matches!(actor.send_blocking(3), Err(SendError::Closed(3))));
	}
}
