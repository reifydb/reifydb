// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicUsize, Ordering};

use reifydb_runtime::sync::mutex::{Mutex, MutexGuard};
use rusqlite::Connection;

pub struct ReadPool {
	pub(crate) conns: Vec<Mutex<Option<Connection>>>,
	next: AtomicUsize,
}

impl ReadPool {
	pub fn new(conns: Vec<Connection>) -> Self {
		Self {
			conns: conns.into_iter().map(|conn| Mutex::new(Some(conn))).collect(),
			next: AtomicUsize::new(0),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.conns.is_empty()
	}

	pub fn acquire(&self) -> MutexGuard<'_, Option<Connection>> {
		let n = self.conns.len();
		let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
		for i in 0..n {
			if let Some(guard) = self.conns[(start + i) % n].try_lock() {
				return guard;
			}
		}
		self.conns[start].lock()
	}

	pub fn shutdown(&self) {
		for slot in &self.conns {
			drop(slot.lock().take());
		}
	}
}
