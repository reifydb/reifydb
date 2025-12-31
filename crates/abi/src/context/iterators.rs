// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

/// Opaque handle to a state iterator (managed by host)
#[repr(C)]
pub struct StateIteratorFFI {
	_opaque: [u8; 0],
}

/// Opaque handle to a store iterator (managed by host)
#[repr(C)]
pub struct StoreIteratorFFI {
	_opaque: [u8; 0],
}
