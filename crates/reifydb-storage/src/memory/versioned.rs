// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::sync::atomic::{AtomicU8, Ordering};
use std::fmt::Debug;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{Version, row::EncodedRow};

const UNINITIALIZED: u8 = 0;
const LOCKED: u8 = 1;
const UNLOCKED: u8 = 2;

#[derive(Debug)]
pub struct VersionedRow {
	pub(crate) op: AtomicU8,
	rows: SkipMap<Version, Option<EncodedRow>>,
}

impl VersionedRow {
	pub(crate) fn new() -> Self {
		Self {
			op: AtomicU8::new(UNINITIALIZED),
			rows: SkipMap::new(),
		}
	}

	pub(crate) fn lock(&self) {
		let mut current = UNLOCKED;
		// Spin lock is ok here because the lock is expected to be held
		// for a very short time. and it is hardly contended.
		loop {
			match self.op.compare_exchange_weak(
				current,
				LOCKED,
				Ordering::SeqCst,
				Ordering::Acquire,
			) {
				Ok(_) => return,
				Err(old) => {
					// If the current state is
					// uninitialized, we can directly
					// return. as we are based on
					// SkipMap, let it to handle concurrent
					// write is engouth.
					if old == UNINITIALIZED {
						return;
					}

					current = old;
				}
			}
		}
	}

	pub(crate) fn unlock(&self) {
		self.op.store(UNLOCKED, Ordering::Release);
	}
}

impl core::ops::Deref for VersionedRow {
	type Target = SkipMap<u64, Option<EncodedRow>>;

	fn deref(&self) -> &Self::Target {
		&self.rows
	}
}
