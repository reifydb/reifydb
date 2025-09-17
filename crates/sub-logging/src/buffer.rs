// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Lock-free buffer for high-performance log collection

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};
use reifydb_core::interface::subsystem::logging::Record;

/// Lock-free buffer for log records
#[derive(Debug)]
pub(crate) struct Buffer {
	/// Channel for sending logs
	sender: Sender<Record>,
	/// Channel for receiving logs
	receiver: Receiver<Record>,
	/// Current buffer size
	size: AtomicUsize,
	/// Maximum buffer capacity
	capacity: usize,
	/// Metrics: Total logs processed
	total_processed: AtomicU64,
	/// Metrics: Total logs dropped due to buffer full
	total_dropped: AtomicU64,
}

impl Buffer {
	/// Create a new log buffer with specified capacity
	pub fn new(capacity: usize) -> Self {
		let (sender, receiver) = bounded(capacity);
		Self {
			sender,
			receiver,
			size: AtomicUsize::new(0),
			capacity,
			total_processed: AtomicU64::new(0),
			total_dropped: AtomicU64::new(0),
		}
	}

	/// Try to push a log record into the buffer
	pub fn try_push(&self, record: Record) -> Result<(), Record> {
		match self.sender.try_send(record) {
			Ok(()) => {
				self.size.fetch_add(1, Ordering::AcqRel);
				self.total_processed.fetch_add(1, Ordering::Relaxed);
				Ok(())
			}
			Err(TrySendError::Full(record)) => {
				self.total_dropped.fetch_add(1, Ordering::Relaxed);
				Err(record)
			}
			Err(TrySendError::Disconnected(record)) => Err(record),
		}
	}

	/// Force push a log record, potentially dropping old logs if buffer is
	/// full
	pub fn force_push(&self, mut record: Record) {
		// Try normal push first
		match self.try_push(record) {
			Ok(()) => return,
			Err(returned_record) => {
				record = returned_record;
				// If buffer is full, try to remove one old item
				// and retry
				if self.receiver.try_recv().is_ok() {
					self.size.fetch_sub(1, Ordering::AcqRel);
					self.total_dropped.fetch_add(1, Ordering::Relaxed);
					// Try again after making space
					let _ = self.try_push(record);
				} else {
					// Couldn't make space, drop the record
					self.total_dropped.fetch_add(1, Ordering::Relaxed);
				}
			}
		}
	}

	/// Drain up to `max_count` records from the buffer
	pub fn drain(&self, max_count: usize) -> Vec<Record> {
		let mut records = Vec::with_capacity(max_count.min(self.size.load(Ordering::Acquire)));

		for _ in 0..max_count {
			match self.receiver.try_recv() {
				Ok(record) => {
					self.size.fetch_sub(1, Ordering::AcqRel);
					records.push(record);
				}
				Err(TryRecvError::Empty) => break,
				Err(TryRecvError::Disconnected) => break,
			}
		}

		records
	}

	/// Drain all available records from the buffer
	pub fn drain_all(&self) -> Vec<Record> {
		self.drain(self.capacity)
	}

	/// Get the current number of buffered logs
	pub fn len(&self) -> usize {
		self.size.load(Ordering::Acquire)
	}

	/// Check if the buffer is full
	pub fn is_full(&self) -> bool {
		self.len() >= self.capacity
	}

	/// Get the buffer capacity
	pub fn capacity(&self) -> usize {
		self.capacity
	}

	/// Get metrics: total logs processed
	pub fn total_processed(&self) -> u64 {
		self.total_processed.load(Ordering::Relaxed)
	}
	/// Get metrics: total logs dropped
	pub fn total_dropped(&self) -> u64 {
		self.total_dropped.load(Ordering::Relaxed)
	}
}
