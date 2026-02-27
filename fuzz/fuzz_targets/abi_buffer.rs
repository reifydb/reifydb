#![no_main]

use libfuzzer_sys::fuzz_target;
use reifydb_abi::data::buffer::BufferFFI;

fuzz_target!(|data: &[u8]| {
    let buf = BufferFFI::from_slice(data);

    // Invariant: from_slice on non-empty data must not be empty
    if !data.is_empty() {
        assert!(!buf.is_empty());
    }

    // Invariant: from_slice on empty data must be empty
    if data.is_empty() {
        assert!(buf.is_empty());
    }

    // Invariant: as_slice roundtrips
    let slice = unsafe { buf.as_slice() };
    assert_eq!(slice, data);
    assert_eq!(buf.len, data.len());

    // Empty buffer invariants
    let empty = BufferFFI::empty();
    assert!(empty.is_empty());
    let empty_slice = unsafe { empty.as_slice() };
    assert!(empty_slice.is_empty());
});
