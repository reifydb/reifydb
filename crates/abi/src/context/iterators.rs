// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Opaque handle to a state iterator (managed by host)
#[repr(C)]
pub struct StateIteratorFFI {
	_opaque: [u8; 0],
}

#[repr(C)]
pub struct StoreIteratorFFI {
	_opaque: [u8; 0],
}
