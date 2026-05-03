// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(reifydb_single_threaded)]
use std::cell::RefCell;
#[cfg(reifydb_single_threaded)]
use std::rc::Rc;

use cfg_if::cfg_if;
#[cfg(not(reifydb_single_threaded))]
use tokio::sync::oneshot;

#[cfg(not(reifydb_single_threaded))]
use super::mailbox::AskError;

cfg_if! {
	if #[cfg(reifydb_single_threaded)] {


		pub struct Reply<T>(Rc<RefCell<Option<T>>>);


		pub struct ReplyReceiver<T>(Rc<RefCell<Option<T>>>);

// SAFETY: DST and WASM are single-threaded. These types never cross thread

		unsafe impl<T> Send for Reply<T> {}
		unsafe impl<T> Sync for Reply<T> {}
		unsafe impl<T> Send for ReplyReceiver<T> {}
		unsafe impl<T> Sync for ReplyReceiver<T> {}


		pub fn reply_channel<T>() -> (Reply<T>, ReplyReceiver<T>) {
			let slot = Rc::new(RefCell::new(None));
			(Reply(Rc::clone(&slot)), ReplyReceiver(slot))
		}

		impl<T> Reply<T> {

			pub fn send(self, value: T) {
				*self.0.borrow_mut() = Some(value);
			}
		}

		impl<T> ReplyReceiver<T> {

			pub fn try_recv(&self) -> Option<T> {
				self.0.borrow_mut().take()
			}
		}
	} else {

		pub struct Reply<T>(oneshot::Sender<T>);


		pub struct ReplyReceiver<T>(oneshot::Receiver<T>);


		pub fn reply_channel<T>() -> (Reply<T>, ReplyReceiver<T>) {
			let (tx, rx) = oneshot::channel();
			(Reply(tx), ReplyReceiver(rx))
		}

		impl<T> Reply<T> {

			pub fn send(self, value: T) {
				let _ = self.0.send(value);
			}
		}

		impl<T> ReplyReceiver<T> {

			pub async fn recv(self) -> Result<T, AskError> {
				self.0.await.map_err(|_| AskError::ResponseClosed)
			}
		}
	}
}
