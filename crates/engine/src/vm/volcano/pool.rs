// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cell::RefCell;

thread_local! {
	static POOL: RefCell<Vec<Vec<u8>>> = RefCell::new(Vec::new());
}

/// Take a zeroed byte buffer suitable for a BitVec of `len` bits.
pub fn take_bytes(len: usize) -> Vec<u8> {
	let byte_count = (len + 7) / 8;
	POOL.with(|pool| {
		let mut pool = pool.borrow_mut();
		if let Some(mut v) = pool.pop() {
			v.clear();
			v.resize(byte_count, 0);
			v
		} else {
			vec![0u8; byte_count]
		}
	})
}

/// Return a byte buffer to the pool for later reuse.
pub fn recycle_bytes(v: Vec<u8>) {
	POOL.with(|pool| {
		pool.borrow_mut().push(v);
	});
}
