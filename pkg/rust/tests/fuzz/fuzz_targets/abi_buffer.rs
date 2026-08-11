// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![no_main]

use libfuzzer_sys::fuzz_target;
use reifydb_abi::data::buffer::ExternCBuffer;

fuzz_target!(|data: &[u8]| {
    let buf = ExternCBuffer::from_slice(data);

    if !data.is_empty() {
        assert!(!buf.is_empty());
    }

    if data.is_empty() {
        assert!(buf.is_empty());
    }

    // SAFETY: `buf` borrows `data`, which outlives this scope, so the pointer and length are valid.
    let slice = unsafe { buf.as_slice() };
    assert_eq!(slice, data);
    assert_eq!(buf.len, data.len());

    let empty = ExternCBuffer::empty();
    assert!(empty.is_empty());
    // SAFETY: an empty `ExternCBuffer` is a valid zero-length buffer, so `as_slice` reads nothing.
    let empty_slice = unsafe { empty.as_slice() };
    assert!(empty_slice.is_empty());
});
