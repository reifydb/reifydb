// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator state: the byte-bounded [`cache`] that owns every operator's resident state, the [`budget`] and lease
//! accounting that bounds it, the [`membership`] index that keeps absence proofs alive across eviction, the
//! [`store`] contract behind it, and the group/horizon/keyspace vocabulary reclamation works in. Not window-specific:
//! ffi, native, distinct and take all route their state through the same cache.

pub mod budget;
pub mod cache;
pub mod group;
pub mod horizon;
pub mod keyspace;
pub mod map;
pub mod membership;
pub mod store;
