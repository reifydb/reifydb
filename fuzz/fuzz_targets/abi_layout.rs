// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use reifydb_abi::data::buffer::BufferFFI;
use reifydb_abi::data::layout::LayoutFFI;

#[derive(Arbitrary, Debug)]
struct LayoutInput {
    bitvec_size: u8,
    field_count: u8,
    data: Vec<u8>,
}

fuzz_target!(|input: LayoutInput| {
    let bitvec_size = input.bitvec_size as usize;
    let field_count = input.field_count as usize;

    let layout = LayoutFFI {
        fields: core::ptr::null(),
        field_count,
        field_names: core::ptr::null(),
        bitvec_size,
        static_section_size: 0,
        alignment: 1,
    };

    let buf = BufferFFI::from_slice(&input.data);

    // Test is_defined for all field indices — must not panic or read OOB
    for i in 0..field_count.saturating_add(1) {
        let result = layout.is_defined(&buf, i);

        // If bitvec_size > data length, byte access would be OOB.
        // is_defined should return false (not UB) when the byte_index
        // falls within bitvec_size but outside the actual buffer.
        let byte_index = i / 8;
        if i >= field_count || buf.is_empty() || byte_index >= bitvec_size {
            assert!(!result, "expected false for out-of-range index {i}");
        }
    }

    // Empty buffer should always return false
    let empty = BufferFFI::empty();
    for i in 0..field_count {
        assert!(!layout.is_defined(&empty, i));
    }
});
